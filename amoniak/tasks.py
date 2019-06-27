# -*- coding: utf-8 -*-
from __future__ import absolute_import

import logging
import urllib2
from datetime import date, datetime, timedelta
from operator import itemgetter

import libsaas
import xmlrpclib
from empowering.utils import make_local_timestamp
from raven import Client

from .amon import AmonConverter
from .empowering_tasks import EmpoweringTasks
from .utils import (
    setup_peek, setup_mongodb, setup_empowering_api,
    Popper, setup_queue
)

sentry = Client()
logger = logging.getLogger('amon')


def enqueue_all_amon_measures(tg_enabled=True, bucket=500):
    if not tg_enabled:
        return

    mongo = setup_mongodb()
    em_tasks = EmpoweringTasks()
    q = setup_queue(name='measures')

    serials = open('serials', 'r')
    for serial in serials:
        meter_name = serial.replace('\n', '').strip()
        if not meter_name.startswith('ZIV00'):
            continue
        filters = {
            'name': meter_name,
            'type': 'day',
            'value': 'a',
            'valid': True,
            'period': 0
        }
        collection = mongo['tg_billing']
        measures = collection.find(filters, {'id': 1})
        measures_to_push = []
        for idx, measure in enumerate(measures):
            if idx and not idx % bucket:
                j = q.enqueue_call(
                    func=em_tasks.push_amon_measures,
                    args=(tg_enabled, measures_to_push,)
                )
                logger.info("Job id:%s | %s/%s/%s" % (
                    j.id, meter_name, idx, bucket)
                )
                measures_to_push = []
            measures_to_push.append(measure['id'])
    mongo.connection.disconnect()
    em_tasks._em.logout()


def enqueue_measures(tg_enabled=True, polisses_ids=[], bucket=500):
    # First get all the contracts that are in sync
    O = setup_peek()
    em_tasks = EmpoweringTasks()

    q = setup_queue(name='measures')

    # TODO: Que fem amb les de baixa? les agafem igualment? només les que
    # TODO: faci menys de X que estan donades de baixa?
    search_params = [('etag', '!=', False),
                     ('state', '=', 'activa'),
                     ('cups.empowering', '=', True)]
    if isinstance(polisses_ids, list) and polisses_ids:
        search_params.append(('name', 'in', polisses_ids))
    pids = O.GiscedataPolissa.search(search_params)

    fields_to_read = ['data_alta']
    date_start = {
        x['id']:x['data_alta']
        for x in O.GiscedataPolissa.read(pids, fields_to_read)
    }

    search_params = [('polissa', 'in', pids)]
    if tg_enabled:
        search_params.append(('tg_cnc_conn', '=', 1))

    cids = O.GiscedataLecturesComptador.search(search_params, context={'active_test': False})
    fields_to_read = ['name', 'empowering_last_measure', 'polissa']

    # Measurement source type filter
    #Name                               codi   subcodi   active
    #----------------------------------+------+---------+--------
    #Telemesura                        | 10   | 00      | t
    #Telemesura corregida              | 11   | 00      | f
    #TPL                               | 20   | 00      | t
    #TPL corregida                     | 21   | 00      | f
    #Visual                            | 30   | 00      | t
    #Visual corregida                  | 31   | 00      | f
    #Estimada                          | 40   | 00      | t
    #Estimada amb històric             | 40   | 01      | t
    #Estimada amb factor dutilització  | 40   | 02      | t
    #Autolectura                       | 50   | 00      | t
    #Telegestió                        | 60   | 00      | t
    #Sense Lectura                     | 99   | 00      | t
    origen_to_search = ['10','11','20','21','30','31','40','50','60']
    origen_ids = O.GiscedataLecturesOrigen.search([('codi','in',origen_to_search)])

    popper = Popper([])

    for comptador in O.GiscedataLecturesComptador.read(cids, fields_to_read):
        try:
            if comptador['polissa'][0] not in date_start:
                continue
            search_params = [
                ('comptador', '=', comptador['id']),
                ('name', '>=', date_start[comptador['polissa'][0]])
            ]

            if tg_enabled:
                tg_name = O.GiscedataLecturesComptador.build_name_tg(comptador['id'])
                search_params += [
                    ('name', '=', tg_name),
                    ('type', '=', 'day'),
                    ('value', '=', 'a'),
                    ('valid', '=', 1),
                    ('period', '=',  0)]

            last_measure = comptador.get('empowering_last_measure')
            logger.info(u"Polissa_id: %s > " % str(comptador['polissa']))
            if not last_measure:
                logger.info(u"No hi ha lectures. Carregar totes")

            else:
                logger.info(u"Última lectura trobada: %s" % last_measure)
                if tg_enabled:
                    search_params.append(('date_end', '>', last_measure))
                else:
                    search_params.append(('write_date', '>', last_measure))

            if tg_enabled:
                measures_ids = O.TgBilling.search(search_params, limit=0, order="date_end asc")
            else:
                search_params.append(('origen_id', 'in', origen_ids))
                measures_ids = O.GiscedataLecturesLectura.search(search_params, limit=0, order="name asc")
            logger.info("S'han trobat %s mesures per pujar\n" % len(measures_ids))
            popper.push(measures_ids)
        except xmlrpclib.ProtocolError as e:
            O = setup_peek()

    pops = popper.pop(bucket)
    while pops:
        j = q.enqueue_call(
            func=em_tasks.push_amon_measures,
            args=(tg_enabled, pops,)
        )
        logger.info("Job id:%s | %s/%s" % (
             j.id, len(pops), len(popper.items))
        )
        pops = popper.pop(bucket)
    em_tasks._em.logout()


def enqueue_new_contracts(tg_enabled, polisses_ids=[], bucket=500):
    search_params = [
        ('polissa.etag', '=', False),
        ('polissa.cups.empowering', '=', True)
    ]
    if tg_enabled:
        search_params.append(('tg_cnc_conn', '=', 1))
    residential_tariffs = [
        '2.0A', '2.0DHA', '2.0DHS', '2.1A', '2.1DHA', '2.1DHS'
    ]
    search_params.append(('polissa.tarifa.name', 'in', residential_tariffs))

    # Enable to force 180 days backwards check instead of online checking
    from_date = (date.today() - timedelta(days=180)).strftime('%Y-%m-%d')
    search_params.append(('polissa.create_date', '<', from_date))
    if isinstance(polisses_ids, list) and polisses_ids:
        search_params.append(('polissa.name', 'in', polisses_ids))

    O = setup_peek()
    em_tasks = EmpoweringTasks()
    cids = O.GiscedataLecturesComptador.search(search_params,
        context={'active_test': False}
    )
    if not cids:
        return
    contracts_ids = [
        x['polissa'][0]
        for x in O.GiscedataLecturesComptador.read(cids, ['polissa'])
    ]
    contracts_ids = list(set(contracts_ids))
    contracts_ids = O.GiscedataPolissa.search([
        ('id', 'in', contracts_ids),
        ('state', 'not in', ('esborrany', 'validar'))
    ])
    popper = Popper(contracts_ids)
    pops = popper.pop(bucket)
    while pops:
        j = em_tasks.push_contracts.delay(pops)
        logger.info("Job id:%s" % j.id)
        pops = popper.pop(bucket)
    em_tasks._em.logout()


def enqueue_contracts(tg_enabled, contracts_id=[]):
    tasks = []
    O = setup_peek()
    em = setup_empowering_api()
    em_tasks = EmpoweringTasks()

    q = setup_queue(name='contracts')

    # Busquem els que hem d'actualitzar
    search_params = [
        ('state', '=', 'activa'),
        ('etag', '!=', False),
        ('cups.empowering', '=', True)
    ]
    if isinstance(contracts_id, list) and contracts_id:
        search_params.append(('name', 'in', contracts_id))
    polisses_ids = O.GiscedataPolissa.search(search_params)

    logger.info("Hay %d polizas para comprobar", len(polisses_ids))
    if not polisses_ids:
        em.logout()
        em_tasks._em.logout()
        return

    fields = ['name', 'etag', 'cups', 'modcontractual_activa', 'comptadors', 'empowering_last_update']
    for polissa in O.GiscedataPolissa.read(polisses_ids, fields):
        try:
            modcons_to_update = []
            is_new_contract = False
            contract = polissa['name']
            modcon_id = O.GiscedataPolissaModcontractual.search([('polissa_id','=',polissa['id']), ('active','=',True)])[0]
            writedate = O.GiscedataPolissaModcontractual.perm_read(modcon_id)[0]['write_date']
            if not polissa['empowering_last_update']:
                is_new_contract = True
            if is_new_contract:
                logger.info("La polissa %s no te la data de pujada informada,"
                            "es torna a pujar" % polissa['name'])
                msg = "Encolando task push_contracts, polissa %s"
                logger.info(msg, polissa['name'])
                job = q.enqueue_call(
                    func=em_tasks.push_contracts,
                    args=([polissa['id']],)
                )
                tasks.append(job)
                continue


            if writedate <= polissa['empowering_last_update']:
                continue

            if not is_new_contract:
                modcons_id = O.GiscedataPolissaModcontractual.search([('polissa_id','=',polissa['id']), ('active','=',True)])
                for modcon_id in modcons_id:
                    w_date = O.GiscedataPolissaModcontractual.perm_read(modcon_id)[0]['write_date']
                    if w_date > polissa['empowering_last_update']:
                        logger.info('La modcontractual %d a actualitzar write_'
                                    'date: %s last_update: %s' % (
                            modcon_id, w_date, polissa['empowering_last_update']))
                        modcons_to_update.append(modcon_id)
                        continue

            modcons_to_update = list(set(modcons_to_update))
            if modcons_to_update:
                msg = "Polissa %s actualitzada a %s després de %s"
                logger.info(msg, polissa['name'], w_date, polissa['empowering_last_update'])
                msg = "Encolando task push_modcontracts, polissa %s"
                logger.info(msg, polissa['name'])
                etag = polissa['etag']
                job = q.enqueue_call(
                    func=em_tasks.push_modcontracts,
                    args=(modcons_to_update, etag,)
                )
                tasks.append(job)

        except xmlrpclib.ProtocolError as e:
            msg = "Ha ocurrido un error inesperado con la conexión al ERP, " \
                  "poliza %s: %s"
            logger.exception(msg, polissa['name'], str(e))
        except (libsaas.http.HTTPError, urllib2.HTTPError) as e:
            msg = "Error inesperado con la conexión con BEEDATA: %s"
            logger.exception(msg, str(e))

    failed_tasks = _check_failed_tasks(tasks)
    errors = [
        "Polissa: {elem[0]}, error: {elem[1]}".format(elem=failed_info)
        for _, failed_info in failed_tasks.iteritems()
    ]
    msg = "Enqueue contracts finalizado, cerrando sesión con BEEDATA" \
          if not errors else "\n".join(errors)
    logger.info(msg)
    em_tasks._em.logout()
    em.logout()


def _check_failed_tasks(tasks):
    '''
    Check if a job has failed, if so return the list with all these jobs.
    '''
    get_polissa_id = itemgetter(0)
    result = {}

    for index, job_ in enumerate(tasks):
        if job_.is_finished:
            try:
                job_.result
            except Exception as e:
                result[job_.id] = (get_polissa_id(job_.args[0]), str(e))
            finally:
                tasks.pop(index)
                tasks = tasks[:]

    return result


def enqueue_remove_contracts(tg_enabled, contracts_id=[]):
    O = setup_peek()
    em = setup_empowering_api()
    em_tasks = EmpoweringTasks(O, em)
    # Busquem els que hem d'actualitzar
    search_params = [
        ('state', '=', 'baixa'),
        ('etag', '!=', False),
        ('cups.empowering', '=', True)
    ]
    if isinstance(contracts_id, list) and contracts_id:
        search_params.append(('name', 'in', contracts_id))
    polisses_ids = O.GiscedataPolissa.search(search_params,
        context={'active_test': False})
    if not polisses_ids:
        em.logout()
        return
    for polissa in O.GiscedataPolissa.read(polisses_ids, ['name', 'etag', 'cups','modcontractual_activa']):
        modcons_to_update = []
        try:
            last_updated = em.contract(polissa['name']).get()['_updated']
            last_updated = make_local_timestamp(last_updated)

            modcons_id = O.GiscedataPolissaModcontractual.search([('polissa_id','=',polissa['id'])],
                context={'active_test': False})
            for modcon_id in modcons_id:
                w_date = O.GiscedataPolissaModcontractual.perm_read(modcon_id)[0]['write_date']
                if w_date > last_updated:
                    logger.info('La modcontractual %d a actualitzar write_'
                                'date: %s last_update: %s' % (
                        modcon_id, w_date, last_updated))
                    modcons_to_update.append(modcon_id)
                    continue

            modcons_to_update = list(set(modcons_to_update))

            if modcons_to_update:
                logger.info('Polissa %s actualitzada a %s després de %s' % (
                    polissa['name'], w_date, last_updated))
                em_tasks.push_modcontracts.delay(
                    modcons_to_update, polissa['etag']
                )
        except xmlrpclib.ProtocolError as e:
            O = setup_peek()
            print e
        except (libsaas.http.HTTPError, urllib2.HTTPError) as e:
            # A 404 is possible if we delete empowering contracts in insight engine
            # but keep etag in our database.
            em = setup_empowering_api()
            print e
            continue
    em.logout()


def enqueue_cch(collection, column, consolidate):
    O = setup_peek()
    em_tasks = EmpoweringTasks()
    mongo_conn = setup_mongodb()
    amon = AmonConverter(O, mongo_conn)

    q = setup_queue(name='api_cch_sender')

    logger.info("Subiendo las curvas %s", collection)

    gp_obj = O.GiscedataPolissa
    filters = [
        ('cups.empowering', '=', True),
        ('state', '=', 'activa'),
        ('active', '=', True),
    ]
    fields_to_read = ['cups', 'data_alta']

    id_polissa_list = gp_obj.search(filters)

    logger.info("Hay %d polizas para subir", len(id_polissa_list))

    for id_contract in id_polissa_list:
        try:
            last_upload_date, id_last_update = __get_last_cch_upload(O, id_contract, column)
            json_to_send, reading_date = amon.cch_to_amon(
                gp_obj.read(id_contract, fields_to_read)['cups'][1],
                last_upload_date,
                collection,
                consolidate
            )
            if json_to_send:
                emp_task = q.enqueue_call(
                    func=em_tasks.push_amon_cch,
                    args=(json_to_send,)
                )
                erp_task = q.enqueue_call(
                    func=em_tasks.update_lectures_comptador,
                    args=(id_last_update, {column: reading_date},),
                    depends_on=emp_task
                )
        except Exception as e:
            msg = "Error subiendo la poliza con id %s: %s"
            logger.exception(msg % (id_contract, str(e)))

    em_tasks._em.logout()
    logger.info("Subidas todas las curvas %s", collection)


def __get_last_cch_upload(O, id_contract, column):
    glc_obj = O.GiscedataLecturesComptador
    id_last_upload = glc_obj.search(
        [('polissa', '=', id_contract), ('active', '=', True)],
        limit=1, order=column + ' desc'
    )[0]

    last_date = glc_obj.read(
        id_last_upload, [column]
    )[column] or '1970-01-01 00:00:00'

    return datetime.strptime(last_date, "%Y-%m-%d %H:%M:%S"), id_last_upload

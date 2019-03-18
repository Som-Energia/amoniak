import logging
from datetime import datetime

import pymongo
from raven import Client

from .amon import AmonConverter, check_response, get_device_serial
from .utils import (
    setup_empowering_api, setup_mongodb, setup_peek, sorted_by_key
)

sentry = Client()
logger = logging.getLogger('amon')


class EmpoweringTasks(object):

    _O = setup_peek()
    _em = setup_empowering_api()

    @sentry.capture_exceptions
    def push_amon_measures(self, tg_enabled, measures_ids):
        """Pugem les mesures a l'Insight Engine
        O: erp connection
        em: emporwering connection
        """

        amon = AmonConverter(self._O)
        start = datetime.now()
        if tg_enabled:
            mongo = setup_mongodb()
            collection = mongo['tg_billing']
            mdbmeasures = collection.find({'id': {'$in': measures_ids}},
                                          {'name': 1, 'id': 1, '_id': 0,
                                           'ai': 1, 'r1': 1, 'date_end': 1},
                                          sort=[('date_end', pymongo.ASCENDING)])
            measures = [x for x in mdbmeasures]
        else:
            fields_to_read = ['comptador', 'name', 'tipus', 'periode', 'lectura']

            measures = self._O.GiscedataLecturesLectura.read(measures_ids, fields_to_read)
            # NOTE: Tricky write_date and end_date rename
            for idx, item in enumerate(measures):
                measure_id = item['id']
                measures[idx]['write_date'] = \
                                              self._O.GiscedataLecturesLectura.perm_read(measure_id)[0]['write_date']
                measures[idx]['date_end'] = measures[idx]['name']

        logger.info("Enviant de %s (id:%s) a %s (id:%s)" % (
            measures[-1]['date_end'], measures[-1]['id'],
            measures[0]['date_end'], measures[0]['id']
        ))
        measures_to_push = amon.measure_to_amon(measures)
        stop = datetime.now()
        logger.info('Mesures transformades en %s' % (stop - start))
        start = datetime.now()

        measures_pushed = self._em.residential_timeofuse_amon_measures().create(measures_to_push)

        # TODO: Pending to check whether all measure were properly commited
        if tg_enabled:
            for measure in measures:
                serial = get_device_serial(last_measure['name'])
                cids = self._O.GiscedataLecturesComptador.search([('name', '=', serial)], context={'active_test': False})
                self._O.GiscedataLecturesComptador.update_empowering_last_measure(cids, '%s' % measure['write_date'])
                mongo.connection.disconnect()
        else:
            for measure in measures:
                self._O.GiscedataLecturesComptador.update_empowering_last_measure(
                    [measure['comptador'][0]], '%s' % measure['write_date']
                )
        stop = datetime.now()
        logger.info('Mesures enviades en %s' % (stop - start))
        logger.info("%s measures creades" % len(measures_pushed))

    @sentry.capture_exceptions
    def push_modcontracts(self, modcons, etag):
        """modcons is a list of modcons to push
        _O: erp connection
        _em: emporwering connection
        """
        amon = AmonConverter(self._O)
        fields_to_read = ['data_inici', 'polissa_id']
        modcons = self._O.GiscedataPolissaModcontractual.read(modcons, fields_to_read)
        modcons = sorted_by_key(modcons, 'data_inici')
        for modcon in modcons:
            amon_data = amon.contract_to_amon(
                modcon['polissa_id'][0],
                {'modcon_id': modcon['id']}
            )[0]
            msg = "Actualizando polissa %s, etag %s"
            logger.info(msg, modcon['polissa_id'][1], etag)
            response = self._em.contract(modcon['polissa_id'][1]).update(amon_data, etag)
            if check_response(response, amon_data):
                etag = response['_etag']

            self._O.GiscedataPolissa.write(modcon['polissa_id'][0], {'etag': etag})

    @sentry.capture_exceptions
    def push_contracts(self, contracts_id):
        amon = AmonConverter(self._O)

        if not isinstance(contracts_id, (list, tuple)):
            contracts_id = [contracts_id]

        for pol in self._O.GiscedataPolissa.read(contracts_id, ['modcontractuals_ids', 'name']):
            logger.info("Polissa %s" % pol['name'])
            cid = pol['id']
            upd = []
            first = True
            try:
                for modcon_id in reversed(pol['modcontractuals_ids']):
                    amon_data = amon.contract_to_amon(
                        cid,
                        {'modcon_id': modcon_id, 'first': first}
                    )[0]
                    if first:
                        response = self._em.contracts().create(amon_data)
                        first = False
                    else:
                        etag = str(upd[-1]['_etag'])
                        response = self._em.contract(pol['name']).update(amon_data, etag)
                    if check_response(response, amon_data):
                        upd.append(response)
            except Exception as e:
                logger.info("Exception id: %s %s" % (pol['name'], str(e)))
                continue
            if upd:
                etag = str(upd[-1]['_etag'])
                logger.info("Polissa id: %s -> etag %s" % (pol['name'], etag))
                self._O.GiscedataPolissa.write(cid, {'etag': etag})
            else:
                logger.info("Polissa id: %s no etag found" % (pol['name']))

    @sentry.capture_exceptions
    def push_amon_cch(self, cch_data):
        self._em.amon_measures().create(cch_data)

    @sentry.capture_exceptions
    def update_lectures_comptador(self, comptador_id, data):
        glc_obj = self._O.GiscedataLecturesComptador
        glc_obj.write(comptador_id, data)

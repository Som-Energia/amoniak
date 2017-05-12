#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from hashlib import sha1
import json
import logging
from datetime import datetime

from .cache import CUPS_CACHE, CUPS_UUIDS
from .utils import recursive_update
from empowering.utils import null_to_none, remove_none, make_uuid, make_utc_timestamp
from .climatic_zones import ine_to_zc 
from .postal_codes import ine_to_dp 

UNITS = {1: '', 1000: 'k'}


logger = logging.getLogger('amon')


def get_device_serial(device_id):
    return device_id[5:].lstrip('0')


def get_street_name(cups):
    street = []
    street_name = u''
    if cups['cpo'] or cups['cpa']:
        street = u'CPO %s CPA %s' % (cups['cpo'], cups['cpa'])
    else:
        if cups['tv']:
            street.append(cups['tv'][1])
        if cups['nv']:
            street.append(cups['nv'])
        street_name += u' '.join(street)
        street = [street_name]
        for f_name, f in [(u'número', 'pnp'), (u'escalera', 'es'),
                          (u'planta', 'pt'), (u'puerta', 'pu')]:
            val = cups.get(f, '')
            if val:
                street.append(u'%s %s' % (f_name, val))
    street_name = ', '.join(street)
    return street_name


class AmonConverter(object):
    def __init__(self, connection):
        self.O = connection

    def get_cups_from_device(self, device_id):
        def get_device_serial(device_id):
            field_to_read = 'name'
            return O.GiscedataLecturesComptador.read([device_id],[field_to_read])[0][field_to_read]

        O = self.O
        # Remove brand prefix and right zeros
        serial = get_device_serial(device_id)
        if serial in CUPS_CACHE:
            return CUPS_CACHE[serial]
        else:
            # Search de meter
            cid = O.GiscedataLecturesComptador.search([
                ('name', '=', serial)
            ], context={'active_test': False})
            if not cid:
                res = False
            else:
                cid = O.GiscedataLecturesComptador.browse(cid[0])
                res = make_uuid('giscedata.cups.ps', cid.polissa.cups.name)
                CUPS_UUIDS[res] = cid.polissa.cups.id
                CUPS_CACHE[serial] = res
            return res

    def profile_to_amon(self, profiles):
        """Return a list of AMON readinds.
        {
            "utilityId": "Utility Id",
            "deviceId": "c1810810-0381-012d-25a8-0017f2cd3574",
            "meteringPointId": "c1759810-90f3-012e-0404-34159e211070",
            "readings": [
                {
                    "type_": "electricityConsumption",
                    "unit": "kWh",
                    "period": "INSTANT",
                },
                {
                    "type_": "electricityKiloVoltAmpHours",
                    "unit": "kVArh",
                    "period": "INSTANT",
                }
            ],
            "measurements": [
                {
                    "type_": "electricityConsumption",
                    "timestamp": "2010-07-02T11:39:09Z", # UTC
                    "value": 7
                },
                {
                    "type_": "electricityKiloVoltAmpHours",
                    "timestamp": "2010-07-02T11:44:09Z", # UTC
                    "value": 6
                }
            ]
        }
        """
        O = self.O
        res = []
        if not hasattr(profiles, '__iter__'):
            profiles = [profiles]
        for profile in profiles:
            mp_uuid = self.get_cups_from_device(profile['name'])
            if not mp_uuid:
                logger.info("No mp_uuid for &s" % profile['name'])
                continue
            device_uuid = make_uuid('giscedata.lectures.comptador', profile['name'])
            res.append({
                "deviceId": device_uuid,
                "meteringPointId": mp_uuid,
                "readings": [
                    {
                        "type":  "electricityConsumption",
                        "unit": "%sWh" % UNITS[profile.get('magn', 1000)],
                        "period": "CUMULATIVE",
                    },
                    {
                        "type": "electricityKiloVoltAmpHours",
                        "unit": "%sVArh" % UNITS[profile.get('magn', 1000)],
                        "period": "CUMULATIVE",
                    }
                ],
                "measurements": [
                    {
                        "type": "electricityConsumption",
                        "timestamp": make_utc_timestamp(profile['date_end']),
                        "value": float(profile['ai'])
                    },
                    {
                        "type": "electricityKiloVoltAmpHours",
                        "timestamp": make_utc_timestamp(profile['date_end']),
                        "value": float(profile['r1'])
                    }
                ]
            })
        return res

    def measure_to_amon(self, measures):
        """Return a list of AMON readinds.

        {
            "utilityId": "Utility Id",
            "deviceId": "c1810810-0381-012d-25a8-0017f2cd3574",
            "meteringPointId": "c1759810-90f3-012e-0404-34159e211070",
            "readings": [
                {
                    "type_": "electricityConsumption",
                    "unit": "kWh",
                    "period": "INSTANT",
                },
                {
                    "type_": "electricityKiloVoltAmpHours",
                    "unit": "kVArh",
                    "period": "INSTANT",
                }
            ],
            "measurements": [
                {
                    "type_": "electricityConsumption",
                    "timestamp": "2010-07-02T11:39:09Z", # UTC
                    "value": 7
                },
                {
                    "type_": "electricityKiloVoltAmpHours",
                    "timestamp": "2010-07-02T11:44:09Z", # UTC
                    "value": 6
                }
            ]
        }
        """
        O = self.O
        res = []
        if not hasattr(measures, '__iter__'):
            measures = [measures]
        for measure in measures:
            mp_uuid = self.get_cups_from_device(measure['comptador'][0])
            if not mp_uuid:
                logger.info("No mp_uuid for &s" % measure['comptador'][0])
                continue
            device_uuid = make_uuid('giscedata.lectures.comptador', measure['comptador'][0])

            period = measure['periode'][1][measure['periode'][1].find("(")+1:measure['periode'][1].find(")")].lower()
            if measure['tipus'] == 'A':
                res.append({
                    "deviceId": device_uuid,
                    "meteringPointId": mp_uuid,
                    "readings": [
                        {
                            "type":  "electricityConsumption",
                            "unit": "%sWh" % UNITS[measure.get('magn', 1000)],
                            "period": "CUMULATIVE",
                        }
                    ],
                    "measurements": [
                        {
                            "type": "electricityConsumption",
                            "timestamp": make_utc_timestamp(measure['date_end']),
                            "values":
                                {
                                    period: float(measure['lectura'])
                                }
                        }
                    ]
                })

            elif measure['tipus'] == 'R':
                res.append({
                    "deviceId": device_uuid,
                    "meteringPointId": mp_uuid,
                    "readings": [
                        {
                            "type": "electricityKiloVoltAmpHours",
                            "unit": "%sVArh" % UNITS[measure.get('magn', 1000)],
                            "period": "CUMULATIVE",
                            "dailyPeriod": period
                        }
                    ],
                    "measurements": [
                        {
                            "type": "electricityKiloVoltAmpHours",
                            "timestamp": make_utc_timestamp(measure['date_end']),
                            "value": float(measure['lectura']),
                            "dailyPeriod": period
                        }
                    ]
                })
        return res

    def sips_measure_to_amon(self, cups, measures):
        """Return a list of AMON readinds.

        {
            "utilityId": "Utility Id",
            "deviceId": "c1810810-0381-012d-25a8-0017f2cd3574",
            "meteringPointId": "c1759810-90f3-012e-0404-34159e211070",
            "readings": [
                {
                    "type_": "electricityConsumption",
                    "unit": "kWh",
                    "period": "INSTANT",
                },
                {
                    "type_": "electricityKiloVoltAmpHours",
                    "unit": "kVArh",
                    "period": "INSTANT",
                }
            ],
            "measurements": [
                {
                    "type_": "electricityConsumption",
                    "timestamp": "2010-07-02T11:39:09Z", # UTC
                    "value": 7
                },
                {
                    "type_": "electricityKiloVoltAmpHours",
                    "timestamp": "2010-07-02T11:44:09Z", # UTC
                    "value": 6
                }
            ]
        }
        """
        O = self.O
        res = []
        if not hasattr(measures, '__iter__'):
            measures = [measures]

        mp_uuid = make_uuid('giscedata.cups.ps', cups)
        device_uuid = mp_uuid
        for measure in measures:
	    for i in range(1,3):
	        period = 'P%d' % i
	        period_key = 'activa_%d' % i
	        res.append({
	            "deviceId": device_uuid,
	            "meteringPointId": mp_uuid,
	            "readings": [
	                {
	                    "type":  "electricityConsumption",
	                    "unit": "%sWh" % UNITS[measure.get('magn', 1000)],
	                    "period": "INSTANT",
	                }
	            ],
	            "measurements": [
	                {
	                    "type": "electricityConsumption",
	                    "timestamp": make_utc_timestamp(measure['data_final']),
	                    "values":
	                        {
	                            period: float(measure[period_key])
	                        }
	                }
	            ]
	        })
        return res

    def device_to_amon(self, device_ids):
        """Convert a device to AMON.

        {
            "utilityId": "Utility Id",
            "externalId": required string UUID,
            "meteringPointId": required string UUID,
            "metadata": {
                "max": "Max number",
                "serial": "Device serial",
                "owner": "empresa/client"
            },
        }
        """
        O = self.O
        res = []
        if not hasattr(device_ids, '__iter__'):
            device_ids = [device_ids]
        for dev_id in device_ids:
            dev = O.GiscedataLecturesComptador.browse(dev_id)
            if dev.propietat == "empresa":
                dev.propietat = "company"
            res.append(remove_none({
                "utilityId": "1",
                "externalId": make_uuid('giscedata.lectures.comptador', dev_id),
                "meteringPointId": make_uuid('giscedata.cups.ps', dev.polissa.cups.name),
                "metadata": {
                   "max": dev.giro,
                   "serial": dev.name,
                   "owner": dev.propietat,
                }
            }))
        return res


    def building_to_amon(self,building_id):
        """ Convert building to AMON

         {
              "buildingConstructionYear": 2014,
              "dwellingArea": 196,
              "propertyType": "primary",
              "buildingType": "Apartment",
              "dwellingPositionInBuilding": "first_floor",
              "dwellingOrientation": "SE",
              "buildingWindowsType": "double_panel",
              "buildingWindowsFrame": "PVC",
              "buildingCoolingSource": "electricity",
              "buildingHeatingSource": "district_heating",
              "buildingHeatingSourceDhw": "gasoil",
              "buildingSolarSystem": "not_installed"
         }
        """
        if not building_id:
            return None

        O = self.O
        building_obj = O.EmpoweringCupsBuilding

        fields_to_read =  ['buildingConstructionYear', 'dwellingArea', 'propertyType', 'buildingType', 'dwellingPositionInBuilding',
                           'dwellingOrientation', 'buildingWindowsType', 'buildingWindowsFrame',
                           'buildingCoolingSource', 'buildingHeatingSource', 'buildingHeatingSourceDhw',
                           'buildingSolarSystem']
        building = building_obj.read(building_id)

        return remove_none(null_to_none({ field: building[field] for field in fields_to_read}))

    def eprofile_to_amon(self,profile_id):
        """ Convert profile to AMON

        {
          "totalPersonsNumber": 3,
          "minorsPersonsNumber": 0,
          "workingAgePersonsNumber": 2,
          "retiredAgePersonsNumber": 1,
          "malePersonsNumber": 2,
          "femalePersonsNumber": 1,
          "educationLevel": {
            "edu_prim": 0,
            "edu_sec": 1,
            "edu_uni": 1,
            "edu_noStudies": 1
        }
        """
        if not profile_id:
            return None

        O = self.O
        profile_obj = O.EmpoweringModcontractualProfile
        fields_to_read = ['totalPersonsNumber', 'minorPersonsNumber', 'workingAgePersonsNumber', 'retiredAgePersonsNumber',
                          'malePersonsNumber', 'femalePersonsNumber', 'eduLevel_prim', 'eduLevel_sec', 'eduLevel_uni',
                          'eduLevel_noStudies']
        profile = profile_obj.read(profile_id)

        return remove_none(null_to_none({
            "totalPersonsNumber": profile['totalPersonsNumber'],
            "minorsPersonsNumber": profile['minorPersonsNumber'],
            "workingAgePersonsNumber": profile['workingAgePersonsNumber'],
            "retiredAgePersonsNumber": profile['retiredAgePersonsNumber'],
            "malePersonsNumber": profile['malePersonsNumber'],
            "femalePersonsNumber": profile['femalePersonsNumber'],
            "educationLevel": {
                "edu_prim": profile['eduLevel_prim'],
                "edu_sec": profile['eduLevel_sec'],
                "edu_uni": profile['eduLevel_uni'],
                "edu_noStudies": profile['eduLevel_noStudies']
            }
        }))


    def service_to_amon(self,service_id):
        """ Convert service to AMON

         {
            "OT701": "p1;P2;px"
         }
        """
        if not service_id:
            return None

        O = self.O
        service_obj = O.EmpoweringModcontractualService
        fields_to_read = ['OT101', 'OT103', 'OT105', 'OT106', 'OT109', 'OT201', 'OT204', 'OT401', 'OT502', 'OT503', 'OT603',
                         'OT603g', 'OT701', 'OT703']
        service = service_obj.read(service_id)

        return remove_none(null_to_none({ field: service[field] for field in fields_to_read}))


    def report_to_amon(self, date_start, partner_id):
        """Convert report to AMON.

        {
            "language": "ca_ES",
            "initialMonth": "201701"
        }
        """
        O = self.O
        partner_obj = O.ResPartner
        partner = partner_obj.read(partner_id, ['lang'])

        initial_month = datetime.strptime(date_start, '%Y-%m-%d').strftime('%Y%m')
        return remove_none({
            "language": partner['lang'],
            "initial_month": initial_month,
            })

    def find_changes(self, modcons_id, field):
        O = self.O
        modcon_obj = O.GiscedataPolissaModcontractual
        fields = ['data_inici', 'data_final', 'potencia', 'tarifa']

        modcons = modcon_obj.read(modcons_id, fields)
        modcons = sorted(modcons, key=lambda k: datetime.strptime(k['data_inici'], '%Y-%m-%d'))

        prev_ = modcons[0]
        modcons_ = []
        for next_ in modcons[1:]:
            if prev_[field] != next_[field]:
                modcons_.append(prev_)
                prev_ = next_
            else:
                prev_['data_final'] = next_['data_final']
        modcons_.append(prev_)
        return modcons_

    def tariff_to_amon(self, modcons_id):
        """ Convert tariff to AMON.
        "tariff_": {
          "tariffId": "tariffID-123",
          "dateStart": "2014-10-11T00:00:00Z",
          "dateEnd": null,
        }
        """
        modcon = self.find_changes(modcons_id, 'tarifa')[-1]
        return remove_none({
          "tariffId": modcon['tarifa'][1],
          "dateStart": make_utc_timestamp(modcon['data_inici']),
          "dateEnd": make_utc_timestamp(modcon['data_final'])
        })

    def power_to_amon(self, modcons_id):
        """ Convert power to AMON.
        "power_": {
          "power": 123,
          "dateStart": "2014-10-11T00:00:00Z",
          "dateEnd": null,
        }
        """
        modcon = self.find_changes(modcons_id, 'potencia')[-1]
        return remove_none({
          "power": int(modcon['potencia'] * 1000),
          "dateStart": make_utc_timestamp(modcon['data_inici']),
          "dateEnd": make_utc_timestamp(modcon['data_final'])
        })

    def tariffHistory_to_amon(self, modcons_id):
        """ Convert tariffHistory to AMON.
        "tariffHistory": [
          {
            "tariffId": "tariffID-122",
            "dateStart": "2013-10-11T16:37:05Z",
            "dateEnd": "2014-10-10T23:59:59Z"
          }
        ]
        """
        return [
            {
                "tariffId": modcon['tarifa'][1],
                "dateStart": make_utc_timestamp(modcon['data_inici']),
                "dateEnd": make_utc_timestamp(modcon['data_final'])
            }
            for modcon in self.find_changes(modcons_id, 'tarifa')[:-1]]

    def powerHistory_to_amon(self, modcons_id):
        """ Convert powerHistory to AMON.
        "powerHistory": [
          {
            "power": 122,
            "dateStart": "2013-10-11T16:37:05Z",
            "dateEnd": "2014-10-10T23:59:59Z"
          }
        ]
        """
        return [
            {
                "power": int(modcon['potencia'] * 1000),
                "dateStart": make_utc_timestamp(modcon['data_inici']),
                "dateEnd": make_utc_timestamp(modcon['data_final'])
            }
            for modcon in self.find_changes(modcons_id, 'potencia')[:-1]]

    def contract_to_amon(self, contract_ids, context=None):
        """Converts contracts to AMON.
        {
          "contractId": "contractId-123",
          "ownerId": "ownerId-123",
          "payerId": "payerId-123",
          "signerId": "signerId-123",
          "power": 123,
          "power_": {
            "power": 123,
            "dateStart": "2014-10-11T00:00:00Z",
            "dateEnd": null,
          },
          "powerHistory": [
            {
              "power": 122,
              "dateStart": "2013-10-11T16:37:05Z",
              "dateEnd": "2014-10-10T23:59:59Z"
            }
          ],
          "dateStart": "2013-10-11T16:37:05Z",
          "dateEnd": null,
          "climaticZone": "climaticZoneId-123",
          "weatherStationId": "weatherStatioId-123",
          "version": 1,
          "activityCode": "activityCode",
          "tariffId": "tariffID-123",
          "tariff_": {
            "tariffId": "tariffID-123",
            "dateStart": "2014-10-11T00:00:00Z",
            "dateEnd": null,
          },
          "tariffHistory": [
            {
              "tariffId": "tariffID-122",
              "dateStart": "2013-10-11T16:37:05Z",
              "dateEnd": "2014-10-10T23:59:59Z"
            }
          ],
          "meteringPointId": "c1759810-90f3-012e-0404-34159e211070",
          "experimentalGroupUser": True,
          "experimentalGroupUserTest": True,
          "activeUser": True,
          "activeUserDate": "2014-10-11T16:37:05Z",
          "customer": {
            "customerId": "customerId-123",
            "address": {
              "buildingId": "building-123",
              "city": "city-123",
              "cityCode": "cityCode-123",
              "countryCode": "ES",
              "country": "Spain",
              "street": "street-123",
              "postalCode": "postalCode-123",
              "province": "Barcelona",
              "provinceCode": "provinceCode-123",
              "parcelNumber": "parcelNumber-123"
            },
            "buildingData": {
              "buildingConstructionYear": 2014,
              "dwellingArea": 196,
              "propertyType": "primary",
              "buildingType": "Apartment",
              "dwellingPositionInBuilding": "first_floor",
              "dwellingOrientation": "SE",
              "buildingWindowsType": "double_panel",
              "buildingWindowsFrame": "PVC",
              "buildingCoolingSource": "electricity",
              "buildingHeatingSource": "district_heating",
              "buildingHeatingSourceDhw": "gasoil",
              "buildingSolarSystem": "not_installed"
            },
            "profile": {
              "totalPersonsNumber": 3,
              "minorsPersonsNumber": 0,
              "workingAgePersonsNumber": 2,
              "retiredAgePersonsNumber": 1,
              "malePersonsNumber": 2,
              "femalePersonsNumber": 1,
              "educationLevel": {
                "edu_prim": 0,
                "edu_sec": 1,
                "edu_uni": 1,
                "edu_noStudies": 1
              }
            },
            "customisedGroupingCriteria": {
              "criteria_1": "CLASS 1",
              "criteria_2": "XXXXXXX",
              "criteria_3": "YYYYYYY"
            },
            "customisedServiceParameters": {
              "OT701": "p1;P2;px"
            }
          },
          "devices": [
            {
              "dateStart": "2013-10-11T16:37:05Z",
              "dateEnd": null,
              "deviceId": "c1810810-0381-012d-25a8-0017f2cd3574"
            }
          ]
        }
        """
        O = self.O
        if not context:
            context = {}
        first = context.get('first', None)

        res = []
        pol = O.GiscedataPolissa
        modcon_obj = O.GiscedataPolissaModcontractual

        cups_obj = O.GiscedataCupsPs
        muni_obj = O.ResMunicipi

        building_obj = O.EmpoweringCupsBuilding
        profile_obj = O.EmpoweringModcontractualProfile
        service_obj = O.EmpoweringModcontractualService

        if not hasattr(contract_ids, '__iter__'):
            contract_ids = [contract_ids]
        fields_to_read = ['modcontractual_activa', 'modcontractuals_ids', 'name', 'cups', 'comptadors', 'state', 'data_alta']
        for polissa in pol.read(contract_ids, fields_to_read):
            if polissa['state'] in ('esborrany', 'validar'):
                continue

            modcon_id = None
            if 'modcon_id' in context:
                modcon_id = context['modcon_id']
            elif polissa['modcontractual_activa']:
                modcon_id = polissa['modcontractual_activa'][0]
            else:
                logger.error("Problema amb la polissa %s" % polissa['name'])
                continue
            modcon = modcon_obj.read(modcon_id)
            modcons_id = polissa['modcontractuals_ids']

            def  get_first(x):
                return x[0] if x else None

            building_id = get_first(building_obj.search([('cups_id', '=', modcon['cups'][0])]))
            profile_id = get_first(profile_obj.search([('modcontractual_id', '=', modcon_id)]))
            service_id = get_first(service_obj.search([('modcontractual_id', '=', modcon_id)]))


            contract = {
                'ownerId': make_uuid('res.partner', modcon['titular'][0]),
                'payerId': make_uuid('res.partner', modcon['pagador'][0]),
                'dateStart': make_utc_timestamp(polissa['data_alta']),
                'dateEnd': make_utc_timestamp(modcon['data_final']),
                'contractId': polissa['name'],
                'tariffId': modcon['tarifa'][1],
                'tariff_': self.tariff_to_amon(modcons_id),
                'tariffHistory': self.tariffHistory_to_amon(modcons_id),
                'power': int(modcon['potencia'] * 1000),
                'power_': self.power_to_amon(modcons_id),
                'powerHistory': self.powerHistory_to_amon(modcons_id),
                'version': int(modcon['name']),
                'climaticZone': self.cups_to_climaticZone(modcon['cups'][0]),
                'activityCode': modcon['cnae'] and modcon['cnae'][1] or None,
                'customer': {
                    'customerId': make_uuid('res.partner', modcon['titular'][0]),
                    'buildingData': self.building_to_amon(building_id),
                    'profile': self.eprofile_to_amon(profile_id),
                    'customisedServiceParameters': self.service_to_amon(service_id)
                },
                'devices': self.devices_to_amon(polissa['comptadors']),
                'report': self.report_to_amon(polissa['data_alta'], modcon['pagador'][0])
            }
            if first:
                contract['devices'].append(
                    self.device_to_amon(
                       '1970-01-01',
                        modcon['data_inici'],
                        modcon['cups'][1]))
            cups = self.cups_to_amon(modcon['cups'][0])
            recursive_update(contract, cups)
            res.append(remove_none(contract, context))
        return res

    def devices_to_amon(self, device_ids):
        compt_obj = self.O.GiscedataLecturesComptador
        devices = []
        comptador_fields = ['data_alta', 'data_baixa']
        for comptador in compt_obj.read(device_ids, comptador_fields):
            devices.append(
                    self.device_to_amon(
                        comptador['data_alta'],
                        comptador['data_baixa'],
                        comptador['id']))
        return devices

    def device_to_amon(self, start_date, end_date, device_id):
        return {
                'dateStart': make_utc_timestamp(start_date),
                'dateEnd': make_utc_timestamp(end_date),
                'deviceId': make_uuid('giscedata.lectures.comptador', device_id)
            }

    def cups_to_amon(self, cups_id):
        cups_obj = self.O.GiscedataCupsPs
        muni_obj = self.O.ResMunicipi
        state_obj = self.O.ResCountryState
        sips_obj = self.O.GiscedataSipsPs

        cups_fields = ['id_municipi', 'tv', 'nv', 'cpa', 'cpo', 'pnp', 'pt',
                       'name', 'es', 'pu', 'dp']
        if 'empowering' in cups_obj.fields_get():
            cups_fields.append('empowering')
        cups = cups_obj.read(cups_id, cups_fields)

        muni_id = cups['id_municipi'][0]
        ine = muni_obj.read(muni_id, ['ine'])['ine']
        state_id = muni_obj.read(muni_id, ['state'])['state'][0]
        state = state_obj.read(state_id, ['code'])['code']
        dp = cups['dp']

        if not dp:
            sips_id = sips_obj.search([('name', '=', cups['name'])])
            if sips_id:
                dp = sips_obj.read(int(sips_id[0]), ['codi_postal'])['codi_postal']
            else:
                if ine in ine_to_dp:
                    dp = ine_to_dp[ine]

        res = {
            'meteringPointId': make_uuid('giscedata.cups.ps', cups['name']),
            'customer': {
                'address': {
                    'city': cups['id_municipi'][1],
                    'cityCode': ine,
                    'countryCode': 'ES',
                    #'street': get_street_name(cups),
                    'postalCode': dp,
                    'provinceCode': state
                }
            },
            'experimentalGroupUserTest': False,
            'experimentalGroupUser': cups.get('empowering', False)
        }
        return res

    def cups_to_climaticZone(self, cups_id):
        cups_obj = self.O.GiscedataCupsPs
        muni_obj = self.O.ResMunicipi
        cups = cups_obj.read(cups_id, ['id_municipi'])
        ine = muni_obj.read(cups['id_municipi'][0], ['ine'])['ine']
        if ine in ine_to_zc.keys():
            return ine_to_zc[ine]
        else:
            return None


def check_response(response, amon_data):
    logger.debug('Handlers: %s Class: %s' % (logger.handlers, logger))
    if response['_status'] != 'OK':
        content = '%s%s' % (json.dumps(amon_data), json.dumps(response))
        hash = sha1(content).hexdigest()[:8]
        logger.error(
            "Empowering response Code: %s - %s" % (response['_status'], hash),
            extra={'data': {
                'amon_data': amon_data,
                'response': response
            }}
        )
        return False
    return True

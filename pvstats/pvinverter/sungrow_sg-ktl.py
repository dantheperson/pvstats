#!/usr/bin/env python

# Copyright 2018 Paul Archer
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from pvstats.pvinverter.base import BasePVInverter

from pymodbus.constants import Defaults
from pymodbus.client.sync import ModbusTcpClient
from pymodbus.client.sync import ModbusSerialClient
from pymodbus.transaction import ModbusSocketFramer
from pymodbus.exceptions import ModbusIOException
from pymodbus.payload import BinaryPayloadDecoder
from SungrowModbusTcpClient import SungrowModbusTcpClient
from datetime import datetime

import serial.rs485

from decimal import Decimal, getcontext
getcontext().prec = 9

import logging
_logger = logging.getLogger(__name__)
# registers found here 
# https://solarclarity.co.uk/wp-content/uploads//2018/12/TI_20180301_String-Inverters_Communication-Protocol_V10_EN.pdf
# https://www.scribd.com/document/456644634/String-Inverters-Communication-Protocol

_register_map = {
  'input': {
    '5001':  {'name': 'tag_nominal_power',                       'scale': Decimal(100),       'units': 'W',      'type': 'uint16'},
    '5002':  {'name': 'tag_output_type',                         'scale': Decimal(1),         'units': '',       'type': 'uint16'},
    '5003':  {'name': 'daily_pv_energy',                         'scale': Decimal(100),       'units': 'Wh',     'type': 'uint16'},
    '5004':  {'name': 'lifetime_pv_energy',                      'scale': Decimal(1000),      'units': 'Wh',     'type': 'uint32'},
    '5005':  {'name': 'lifetime_pv_energy_2',                    'scale': Decimal(1000),      'units': 'Wh',     'type': 'uint32'},
    '5006':  {'name': 'lifetime_runtime',                        'scale': Decimal(1),         'units': 'h',      'type': 'uint32'},
    '5007':  {'name': 'lifetime_runtime_2',                      'scale': Decimal(1),         'units': 'h',      'type': 'uint32'},
    '5008':  {'name': 'internal_temp',                           'scale': Decimal('0.1'),     'units': 'C',      'type': 'int16'},
    '5009':  {'name': 'apparent_power',                          'scale': Decimal(1),         'units': 'VA',     'type': 'uint32'},
    '5010':  {'name': 'apparent_power_2',                        'scale': Decimal(1),         'units': 'VA',     'type': 'uint32'},
    '5011':  {'name': 'pv1_voltage',                             'scale': Decimal('0.1'),     'units': 'V',      'type': 'uint16'},
    '5012':  {'name': 'pv1_current',                             'scale': Decimal('0.1'),     'units': 'A',      'type': 'uint16'},
    '5013':  {'name': 'pv2_voltage',                             'scale': Decimal('0.1'),     'units': 'V',      'type': 'uint16'},
    '5014':  {'name': 'pv2_current',                             'scale': Decimal('0.1'),     'units': 'A',      'type': 'uint16'},
    '5017':  {'name': 'total_pv_power',                          'scale': Decimal(1),         'units': 'W',      'type': 'uint32'},
    '5018':  {'name': 'total_pv_power_2',                        'scale': Decimal(1),         'units': 'W',      'type': 'uint32'},
    '5019':  {'name': 'grid_voltage_A',                          'scale': Decimal('0.1'),     'units': 'V',      'type': 'uint16'},
    '5020':  {'name': 'grid_voltage_B',                          'scale': Decimal('0.1'),     'units': 'V',      'type': 'uint16'},
    '5021':  {'name': 'grid_voltage_C',                          'scale': Decimal('0.1'),     'units': 'V',      'type': 'uint16'},
    '5022':  {'name': 'inverter_current_A',                      'scale': Decimal('0.1'),     'units': 'A',      'type': 'uint16'},
    '5023':  {'name': 'inverter_current_B',                      'scale': Decimal('0.1'),     'units': 'A',      'type': 'uint16'},
    '5024':  {'name': 'inverter_current_C',                      'scale': Decimal('0.1'),     'units': 'A',      'type': 'uint16'},
    '5031':  {'name': 'active_power',                            'scale': Decimal(1),         'units': 'W',      'type': 'uint32'},
    '5032':  {'name': 'active_power_2',                          'scale': Decimal(1),         'units': 'W',      'type': 'uint32'},
    '5033':  {'name': 'reactive_power',                          'scale': Decimal(1),         'units': 'VAR',    'type': 'int32'},
    '5034':  {'name': 'reactive_power_2',                        'scale': Decimal(1),         'units': 'VAR',    'type': 'int32'},
    '5035':  {'name': 'power_factor',                            'scale': Decimal('0.001'),   'units': '',       'type': 'int16'},
    '5036':  {'name': 'grid_frequency',                          'scale': Decimal('0.1'),     'units': 'Hz',     'type': 'uint16'},
    '5038':  {'name': 'work_state',                              'scale': Decimal(1),         'units': '',       'type': 'uint16'},
    '5039':  {'name': 'fault_year',                              'scale': Decimal(1),         'units': '',       'type': 'uint16'},
    '5040':  {'name': 'fault_month',                             'scale': Decimal(1),         'units': '',       'type': 'uint16'},
    '5041':  {'name': 'fault_day',                               'scale': Decimal(1),         'units': '',       'type': 'uint16'},
    '5042':  {'name': 'fault_hour',                              'scale': Decimal(1),         'units': '',       'type': 'uint16'},
    '5043':  {'name': 'fault_minute',                            'scale': Decimal(1),         'units': '',       'type': 'uint16'},
    '5044':  {'name': 'fault_second',                            'scale': Decimal(1),         'units': '',       'type': 'uint16'},
    '5045':  {'name': 'tag_fault_code',                          'scale': Decimal(1),         'units': '',       'type': 'uint16'},
    '5049':  {'name': 'tag_nominal_reactive_power',              'scale': Decimal(100),       'units': 'VA',     'type': 'uint16'},
    '5071':  {'name': 'ground_impedance',                        'scale': Decimal(1000),      'units': 'Ohm',    'type': 'uint16'},
    # '5081':  {'name': 'work_state_2',                            'scale': Decimal(1),         'units': '',       'type': 'uint32'},
    # '5082':  {'name': 'work_state_3',                            'scale': Decimal(1),         'units': '',       'type': 'uint32'},
    '5083':  {'name': 'meter_power',                             'scale': Decimal(1),         'units': 'W',      'type': 'int32'},
    '5084':  {'name': 'meter_power_2',                           'scale': Decimal(1),         'units': 'W',      'type': 'int32'},
    '5085':  {'name': 'meter_power_A',                           'scale': Decimal(1),         'units': 'W',      'type': 'int32'},
    '5086':  {'name': 'meter_power_A_2',                         'scale': Decimal(1),         'units': 'W',      'type': 'int32'},
    '5087':  {'name': 'meter_power_B',                           'scale': Decimal(1),         'units': 'W',      'type': 'int32'},
    '5088':  {'name': 'meter_power_B_2',                         'scale': Decimal(1),         'units': 'W',      'type': 'int32'},
    '5089':  {'name': 'meter_power_C',                           'scale': Decimal(1),         'units': 'W',      'type': 'int32'},
    '5090':  {'name': 'meter_power_C_2',                         'scale': Decimal(1),         'units': 'W',      'type': 'int32'},
    '5091':  {'name': 'load_power',                              'scale': Decimal(1),         'units': 'W',      'type': 'int32'},
    '5092':  {'name': 'load_power_2',                            'scale': Decimal(1),         'units': 'W',      'type': 'int32'},
    '5093':  {'name': 'daily_export_energy',                     'scale': Decimal(100),       'units': 'Wh',     'type': 'uint32'},
    '5094':  {'name': 'daily_export_energy_2',                   'scale': Decimal(100),       'units': 'Wh',     'type': 'uint32'},
    '5095':  {'name': 'lifetime_export_energy',                  'scale': Decimal(100),       'units': 'Wh',     'type': 'uint32'},
    '5096':  {'name': 'lifetime_export_energy_2',                'scale': Decimal(100),       'units': 'Wh',     'type': 'uint32'},
    '5097':  {'name': 'daily_import_energy',                     'scale': Decimal(100),       'units': 'Wh',     'type': 'uint32'},
    '5098':  {'name': 'daily_import_energy_2',                   'scale': Decimal(100),       'units': 'Wh',     'type': 'uint32'},
    '5099':  {'name': 'lifetime_import_energy',                  'scale': Decimal(100),       'units': 'Wh',     'type': 'uint32'},
    '5100':  {'name': 'lifetime_import_energy_2',                'scale': Decimal(100),       'units': 'Wh',     'type': 'uint32'},
    '5101':  {'name': 'daily_direct_consumption_energy',         'scale': Decimal(100),       'units': 'Wh',     'type': 'uint32'},
    '5102':  {'name': 'daily_direct_consumption_energy_2',       'scale': Decimal(100),       'units': 'Wh',     'type': 'uint32'},
    '5103':  {'name': 'lifetime_direct_consumption_energy',      'scale': Decimal(100),       'units': 'Wh',     'type': 'uint32'},
    '5104':  {'name': 'lifetime_direct_consumption_energy_2',    'scale': Decimal(100),       'units': 'Wh',     'type': 'uint32'},
    '5113':  {'name': 'daily_runtime',                           'scale': Decimal(1),         'units': 'min',    'type': 'uint16'}, 
    '5114':  {'name': 'tag_country',                             'scale': Decimal(1),         'units': 'UNK',    'type': 'uint16'},
    '5128':  {'name': 'monthly_energy',                          'scale': Decimal(100),       'units': 'Wh',     'type': 'uint32'},
    '5129':  {'name': 'monthly_energy_2',                        'scale': Decimal(100),       'units': 'Wh',     'type': 'uint32'},
    '5144':  {'name': 'lifetime_energy_yeild',                   'scale': Decimal(100),       'units': 'Wh',     'type': 'uint32'},
    '5145':  {'name': 'lifetime_energy_yeild_2',                 'scale': Decimal(100),       'units': 'Wh',     'type': 'uint32'},
    '5146':  {'name': 'negative_voltage_to_ground',              'scale': Decimal('0.1'),     'units': 'V',      'type': 'int16'},
    '5147':  {'name': 'bus_voltage',                             'scale': Decimal('0.1'),     'units': 'V',      'type': 'uint16'},  
    # '5148':  {'name': 'grid_frequency_fine',                     'scale': Decimal('0.01'),     'units': 'Hz',    'type': 'uint16'},  
  },
  'holding': {
    '5000':  {'name': 'date_year',         'scale': 1,               'units': 'year',      'type': 'uint16'},
    '5001':  {'name': 'date_month',        'scale': 1,              'units': 'month',      'type': 'uint16'},
    '5002':  {'name': 'date_day',          'scale': 1,              'units': 'day',        'type': 'uint16'},
    '5003':  {'name': 'date_hour',         'scale': 1,              'units': 'hour',       'type': 'uint16'},
    '5004':  {'name': 'date_minute',       'scale': 1,              'units': 'minute',     'type': 'uint16'},
    '5005':  {'name': 'date_second',       'scale': 1,              'units': 'second',     'type': 'uint16'},
  }
}

class PVInverter_SunGrow(BasePVInverter):
  def __init__(self, cfg, **kwargs):
    super(PVInverter_SunGrow, self).__init__()
    self.client = SungrowModbusTcpClient.SungrowModbusTcpClient(host=cfg['host'],               port=cfg['port'],
                                  timeout=3,
                                  RetryOnEmpty=True,         retries=3)
    if cfg.get('register_map') is None:
      self._register_map = _register_map
    else:
      self._register_map = cfg.get('register')
  def connect(self):
    self.client.connect()

  def close(self):
    self.client.close()

  def read(self):
    """Reads the PV inverters status"""

    # Read holding and input registers in groups aligned on the 100
    for func in self._register_map:
      start = -1
      for k in sorted(self._register_map[func].keys()):
        group  = int(k) - int(k) % 100
        if (start <= group):
          self._load_registers(func, group, 100)
          start = group + 100

    # Manually calculate the power and the timestamps
    self.registers['pv1_power'] = round(self.registers['pv1_current'] * self.registers['pv1_voltage'])
    self.registers['pv2_power'] = round(self.registers['pv2_current'] * self.registers['pv2_voltage'])
    self.registers['timestamp'] = datetime(self.registers['date_year'],   self.registers['date_month'],
                                           self.registers['date_day'],    self.registers['date_hour'],
                                           self.registers['date_minute'], self.registers['date_second']).timestamp()

  def _2x_16_to_32(self,int16_1,int16_2):
    if int16_1 < 0 and int16_2 < 0:
      tag = 3
      sign = -1
    elif int16_1 >= 0 and int16_2 >= 0:
      tag = 2
      sign = 1
    else:
      raise ArithmeticError("Signs don't match for ints")
    iint16_1 = int(int16_1)
    iint16_2 = int(int16_2)
    hxint16_1 = hex(abs(iint16_1))
    hxint16_2 = hex(abs(iint16_2))
    hx16_1 = str(hxint16_1)[tag:]
    hx16_2 = str(hxint16_2)[tag:]
    hx32 = hx16_1 + hx16_2
    try:
      int32 = int(hx32,16) * sign
    except:
      pass
    return int32

  def _load_registers(self,func,start,count=100):
    try:
      if func == 'input':
        rq = self.client.read_input_registers(start, count, unit=0x01)
      elif func == 'holding':
        # Holding registers need an offset
        start = start - 1
        rq = self.client.read_holding_registers(start, count, unit=0x01)
      else:
        raise Exception("Unknown register type: {}".format(type))


      if isinstance(rq, ModbusIOException):
        _logger.error("Error: {}".format(rq))
        raise Exception("ModbusIOException")

      for x in range(0, count):
        key  = str(start + x + 1)
        val  = rq.registers[x]

        if key in self._register_map[func]:
          reg = self._register_map[func][key]
          reg_name = reg.get('name')
          reg_scale = reg.get('scale')
          reg_type = reg.get('type')
          if reg_type == 'int16' and val >= 2**15:
            self.registers[reg_name] = (val - 2**16) * reg_scale
          elif reg_type == 'int32' and val >= 2**15:
            self.registers[reg_name] = (val - 2**16) * reg_scale
          else:
            self.registers[reg_name] = val * reg_scale
          if reg_name.endswith('_2'):
            reg_2 = self.registers[reg_name] / reg_scale
            reg_name_1 = reg_name[0:-2]
            reg_1 = self.registers[reg_name_1] / reg_scale
            self.registers[reg_name[0:-2]] = self._2x_16_to_32(reg_2 , reg_1 ) * reg_scale
            self.registers.pop(reg_name)


    except Exception as err:
      _logger.error("Error: %s" % err)
      _logger.debug("{}, start: {}, count: {}".format(type, start, count))
      raise

class PVInverter_SunGrowRTU(PVInverter_SunGrow):
  def __init__(self, cfg, **kwargs):
    super(PVInverter_SunGrow, self).__init__()

    # Configure the Modbus Remote Terminal Unit settings
    self.client = ModbusSerialClient(method='rtu', port=cfg['dev'], timeout=0.5,
                                     stopbits = 1, bytesize =8, parity='N', baudrate=9600)

  def connect(self):
    # Connect then configure the port
    self.client.connect()

    # Configure the RS485 port - This seems not needed
    #rs485_mode = serial.rs485.RS485Settings(delay_before_tx = 0, delay_before_rx = 0,
    #                                        rts_level_for_tx=True, rts_level_for_rx=False,
    #                                        loopback=False)
    #self.client.socket.rs485_mode = rs485_mode


#-----------------
# Exported symbols
#-----------------
__all__ = [
  "PVInverter_SunGrow", "PVInverter_SunGrowRTU"
]

# vim: set expandtab ts=2 sw=2:

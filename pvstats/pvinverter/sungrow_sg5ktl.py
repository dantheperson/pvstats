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

_register_map = {
  'input': {
    '5001':  {'name': '5001',              'scale': Decimal('1'),   'units': 'UNK'},
    '5002':  {'name': '5002',              'scale': Decimal('1'),   'units': 'UNK'},
    '5003':  {'name': 'daily_pv_power',    'scale': Decimal(100),   'units': 'W'},
    '5004':  {'name': 'lifetime_pv_power', 'scale': Decimal(1),     'units': 'kW'},
    '5005':  {'name': '5005',              'scale': Decimal('1'),   'units': 'UNK'},
    '5006':  {'name': '5006',              'scale': Decimal('1'),   'units': 'UNK'},
    '5007':  {'name': '5007',              'scale': Decimal('1'),   'units': 'UNK'},
    '5008':  {'name': 'internal_temp',     'scale': Decimal('0.1'), 'units': 'C'},
    '5009':  {'name': '5009',              'scale': Decimal('1'),   'units': 'UNK'},
    '5010':  {'name': '5010',              'scale': Decimal('1'),   'units': 'UNK'},    
    '5011':  {'name': 'pv1_voltage',       'scale': Decimal('0.1'), 'units': 'V'},
    '5012':  {'name': 'pv1_current',       'scale': Decimal('0.1'), 'units': 'A'},
    '5013':  {'name': 'pv2_voltage',       'scale': Decimal('0.1'), 'units': 'V'},
    '5014':  {'name': 'pv2_current',       'scale': Decimal('0.1'), 'units': 'A'},
    '5017':  {'name': 'total_pv_power',    'scale': Decimal(1),     'units': 'W'},
    '5018':  {'name': '5018',              'scale': Decimal('1'),   'units': 'UNK'},
    '5019':  {'name': 'grid_voltage',      'scale': Decimal('0.1'), 'units': 'V'},
    '5020':  {'name': '5020',              'scale': Decimal('1'),   'units': 'UNK'},
    '5021':  {'name': '5021',              'scale': Decimal('1'),   'units': 'UNK'},
    '5022':  {'name': 'inverter_current',  'scale': Decimal('0.1'), 'units': 'A'},
    '5023':  {'name': '5023',              'scale': Decimal('1'),   'units': 'UNK'},
    '5024':  {'name': '5024',              'scale': Decimal('1'),   'units': 'UNK'},
    '5031':  {'name': '5031',              'scale': Decimal('1'),   'units': 'UNK'},
    '5032':  {'name': '5032',              'scale': Decimal('1'),   'units': 'UNK'},
    '5033':  {'name': '5033',              'scale': Decimal('1'),   'units': 'UNK'},
    '5035':  {'name': '5035',              'scale': Decimal('1'),   'units': 'UNK'},
    '5036':  {'name': 'grid_frequency',    'scale': Decimal('0.1'), 'units': 'Hz'},
    '5037':  {'name': '5037',              'scale': Decimal('1'),   'units': 'UNK'},
    '5038':  {'name': '5038',              'scale': Decimal('1'),   'units': 'UNK'},
    '5039':  {'name': '5039',              'scale': Decimal('1'),   'units': 'UNK'},
    '5040':  {'name': '5040',              'scale': Decimal('1'),   'units': 'UNK'},
    '5041':  {'name': '5041',              'scale': Decimal('1'),   'units': 'UNK'},
    '5042':  {'name': '5042',              'scale': Decimal('1'),   'units': 'UNK'},
    '5043':  {'name': '5043',              'scale': Decimal('1'),   'units': 'UNK'},
    '5044':  {'name': '5044',              'scale': Decimal('1'),   'units': 'UNK'},
    '5045':  {'name': '5045',              'scale': Decimal('1'),   'units': 'UNK'},
    '5046':  {'name': '5046',              'scale': Decimal('1'),   'units': 'UNK'},
    '5047':  {'name': '5047',              'scale': Decimal('1'),   'units': 'UNK'},
    '5049':  {'name': '5049',              'scale': Decimal('1'),   'units': 'UNK'},
    '5071':  {'name': '5071',              'scale': Decimal('1'),   'units': 'UNK'},
    '5077':  {'name': '5077',              'scale': Decimal('1'),   'units': 'UNK'},
    '5078':  {'name': '5078',              'scale': Decimal('1'),   'units': 'UNK'},
    '5079':  {'name': '5079',              'scale': Decimal('1'),   'units': 'UNK'},
    '5080':  {'name': '5080',              'scale': Decimal('1'),   'units': 'UNK'},
    '5081':  {'name': '5081',              'scale': Decimal('1'),   'units': 'UNK'},
    '5082':  {'name': '5082',              'scale': Decimal('1'),   'units': 'UNK'},
    '5083':  {'name': '5083',              'scale': Decimal('1'),   'units': 'UNK'},
    '5084':  {'name': '5084',              'scale': Decimal('1'),   'units': 'UNK'},
    '5085':  {'name': '5085',              'scale': Decimal('1'),   'units': 'UNK'},
    '5086':  {'name': '5086',              'scale': Decimal('1'),   'units': 'UNK'},
    '5087':  {'name': '5087',              'scale': Decimal('1'),   'units': 'UNK'},
    '5089':  {'name': '5089',              'scale': Decimal('1'),   'units': 'UNK'},
    '5090':  {'name': '5090',              'scale': Decimal('1'),   'units': 'UNK'},
    '5091':  {'name': '5091',              'scale': Decimal('1'),   'units': 'UNK'},
    '5092':  {'name': '5092',              'scale': Decimal('1'),   'units': 'UNK'},
    '5093':  {'name': '5093',              'scale': Decimal('1'),   'units': 'UNK'},
    '5094':  {'name': '5094',              'scale': Decimal('1'),   'units': 'UNK'},
    '5095':  {'name': '5095',              'scale': Decimal('1'),   'units': 'UNK'},
    '5096':  {'name': '5096',              'scale': Decimal('1'),   'units': 'UNK'},
    '5097':  {'name': '5097',              'scale': Decimal('1'),   'units': 'UNK'},
    '5098':  {'name': '5098',              'scale': Decimal('1'),   'units': 'UNK'},
    '5099':  {'name': '5099',              'scale': Decimal('1'),   'units': 'UNK'},    
  },
  'holding': {
    '5000':  {'name': 'date_year',         'scale': 1,              'units': 'year'},
    '5001':  {'name': 'date_month',        'scale': 1,              'units': 'month'},
    '5002':  {'name': 'date_day',          'scale': 1,              'units': 'day'},
    '5003':  {'name': 'date_hour',         'scale': 1,              'units': 'hour'},
    '5004':  {'name': 'date_minute',       'scale': 1,              'units': 'minute'},
    '5005':  {'name': 'date_second',       'scale': 1,              'units': 'second'},
  }
}

class PVInverter_SunGrow(BasePVInverter):
  def __init__(self, cfg, **kwargs):
    super(PVInverter_SunGrow, self).__init__()
    self.client = SungrowModbusTcpClient.SungrowModbusTcpClient(host=cfg['host'],               port=cfg['port'],
                                  timeout=3,
                                  RetryOnEmpty=True,         retries=3)

  def connect(self):
    self.client.connect()

  def close(self):
    self.client.close()

  def read(self):
    """Reads the PV inverters status"""

    # Read holding and input registers in groups aligned on the 100
    for func in _register_map:
      start = -1
      for k in sorted(_register_map[func].keys()):
        group  = int(k) - int(k) % 100
        if (start < group):
          self._load_registers(func, group, 100)
          start = group + 100

    # Manually calculate the power and the timestamps
    self.registers['pv1_power'] = round(self.registers['pv1_current'] * self.registers['pv1_voltage'])
    self.registers['pv2_power'] = round(self.registers['pv2_current'] * self.registers['pv2_voltage'])
    self.registers['timestamp'] = datetime(self.registers['date_year'],   self.registers['date_month'],
                                           self.registers['date_day'],    self.registers['date_hour'],
                                           self.registers['date_minute'], self.registers['date_second']).timestamp()

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

        if key in _register_map[func]:
          reg = _register_map[func][key]
          self.registers[reg['name']] = val * reg['scale']


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

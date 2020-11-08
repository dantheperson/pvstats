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

_register_map = {
  'input': {
    '5001':  {'name': 'nomininal_power',   'scale': Decimal(100),   'units': 'W'},
    '5002':  {'name': 'output_type',       'scale': Decimal(1),     'units': ''},
    '5003':  {'name': 'daily_pv_power',    'scale': Decimal(100),   'units': 'Wh'},
    '5004':  {'name': 'lifetime_pv_power', 'scale': Decimal(1000),  'units': 'Wh'},
    '5005':  {'name': 'lifetime_pv_power_2','scale': Decimal(1000), 'units': 'Wh'},
    '5006':  {'name': 'lifetime_runtime',  'scale': Decimal(1),     'units': 'h'},
    '5007':  {'name': 'lifetime_runtime_2','scale': Decimal(1),     'units': 'h'},
    '5008':  {'name': 'internal_temp',     'scale': Decimal('0.1'), 'units': 'C'},
    '5009':  {'name': 'apparent_power',    'scale': Decimal(1),     'units': 'VA'},
    '5010':  {'name': 'apparent_power_2',  'scale': Decimal(1),     'units': 'VA'},  
    '5011':  {'name': 'pv1_voltage',       'scale': Decimal('0.1'), 'units': 'V'},
    '5012':  {'name': 'pv1_current',       'scale': Decimal('0.1'), 'units': 'A'},
    '5013':  {'name': 'pv2_voltage',       'scale': Decimal('0.1'), 'units': 'V'},
    '5014':  {'name': 'pv2_current',       'scale': Decimal('0.1'), 'units': 'A'},
    '5017':  {'name': 'total_pv_power',    'scale': Decimal(1),     'units': 'W'},
    '5018':  {'name': 'total_pv_power_2',  'scale': Decimal(1),     'units': 'W'},
    '5019':  {'name': 'grid_voltage_A',    'scale': Decimal('0.1'), 'units': 'V'},
    '5020':  {'name': 'grid_voltage_B',    'scale': Decimal('0.1'), 'units': 'V'},
    '5021':  {'name': 'grid_voltage_C',    'scale': Decimal('0.1'), 'units': 'V'},
    '5022':  {'name': 'inverter_current_A','scale': Decimal('0.1'), 'units': 'A'},
    '5023':  {'name': 'inverter_current_B','scale': Decimal('0.1'), 'units': 'A'},
    '5024':  {'name': 'inverter_current_C','scale': Decimal('0.1'), 'units': 'A'},
    '5031':  {'name': 'active_power',      'scale': Decimal(1),     'units': 'W'},
    '5032':  {'name': 'active_power_2',    'scale': Decimal(1),     'units': 'W'},
    '5031':  {'name': 'reactive_power',    'scale': Decimal(1),     'units': 'W'},
    '5032':  {'name': 'reactive_power_2',  'scale': Decimal(1),     'units': 'W'},
    '5035':  {'name': 'power_factor',      'scale': Decimal('0.001'),'units': ''},
    '5036':  {'name': 'grid_frequency',    'scale': Decimal('0.1'), 'units': 'Hz'},
    '5038':  {'name': 'work_state',        'scale': Decimal(1),     'units': ''},
    '5039':  {'name': 'fault_year',        'scale': Decimal(1),     'units': ''},
    '5040':  {'name': 'fault_month',       'scale': Decimal(1),     'units': ''},
    '5041':  {'name': 'fault_day',         'scale': Decimal(1),     'units': ''},
    '5042':  {'name': 'fault_hour',        'scale': Decimal(1),     'units': ''},
    '5043':  {'name': 'fault_minute',      'scale': Decimal(1),     'units': ''},
    '5044':  {'name': 'fault_second',      'scale': Decimal(1),     'units': ''},
    '5045':  {'name': 'fault_code',        'scale': Decimal(1),     'units': ''},
    '5049':  {'name': 'nom_react_power',   'scale': Decimal(100),   'units': 'VA'},
    '5071':  {'name': 'ground_impedance',  'scale': Decimal(1000),  'units': 'Ohm'},
    '5081':  {'name': 'work_state_2',      'scale': Decimal(1),     'units': ''},
    '5082':  {'name': 'work_state_3',      'scale': Decimal(1),     'units': ''},
    '5113':  {'name': 'daily_runtime',     'scale': Decimal(1),     'units': 'min'}, 
    '5114':  {'name': 'country',           'scale': Decimal(1),     'units': 'UNK'},
    '5128':  {'name': 'monthly_power',     'scale': Decimal(100),   'units': 'W'},
    '5129':  {'name': 'monthly_power_2',   'scale': Decimal(100),   'units': 'W'},
    '5146':  {'name': 'neg_voltage_to_gnd','scale': Decimal('0.1'), 'units': 'V'},
    '5147':  {'name': 'bus_voltage',       'scale': Decimal('0.1'), 'units': 'V'},   
    '5148':  {'name': 'grid_freq',         'scale': Decimal('0.1'), 'units': 'Hz'},       
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

        if key in self._register_map[func]:
          reg = self._register_map[func][key]
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

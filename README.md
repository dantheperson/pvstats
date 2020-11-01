# PVStats

Photovoltaic Inverter Statistics Scanner and Uploader

PVStats is a tool used to connect to your inverter using Modbus TCP,
scan for various power figures, and then upload the data to various report services.

Currently supported inverters include:
* Sungrow SG5KTL (TCP and RS485)
* Fronius (WiFi)
* SolaX X1-5.0-T (WiFi)
* Sungrow SH5K-20
* TODO: SMA Sunny Boy

Currently supported reporting methods include:
* PVOutput.org
* MQTT
* InfluxDB
* TODO: dweet.io

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

### Installing

To install the PVStats via pip (Python Package Index)

```
pip install pvstats
```

### Running

First modify `pvstats.conf` with your inverter settings, and also pvoutput.org, MQTT or InfluxDB settings

```
/usr/bin/pvstats -f pvstats.conf
```
## Docker

To deploy a container:
* Create a `pvstats.conf` based on `pvstats.conf.example`
* Run the container with docker run

```
docker run -v /path/to/config:/config damianolombardo/pvstats
```

## Deployment

To deploy this on a live system
* Modify `/etc/pvstats.conf` with your inverter and reporting settings
* Start the service via systemd

Enable the service to start on reboot
```
sudo systemctl enable pvstats.service
```

Immediately  start the service via systemd
```
sudo systemctl start pvstats.service
```

## Built with help from the following projects

* [Pymodbus](https://github.com/riptideio/pymodbus/) - Python Modbus client
* [SolarIOT](https://github.com/meltaxa/solariot/) - Solar PV Inverter to InfluxDB
* [sungrow2pvoutput](https://github.com/kronicd/sungrow2pvoutput) - Sungrow Website scrapper to pvoutput.org

## Contributing

Please read [CONTRIBUTING.md](https://github.com/damianolombardo/pvstats/CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## Versioning

We use [SemVer](http://semver.org/) for versioning. For the versions available, see the [tags on this repository](https://github.com/damianolombardo/pvstats).

## Authors of README.md

* **Damiano Lombardo** - *Modified for pvstats* - [PVStats](https://github.com/damianolombardo/pvstats)
* **Paul Archer** - *Modified for pvstats* - [PVStats](https://github.com/ptarcher/pvstats)
* **Billie Thompson** - *Initial work* - [PurpleBooth](https://github.com/PurpleBooth)

See also the list of [contributors](https://github.com/damianolombardo/pvstats/contributors) who participated in this project.

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details

## Acknowledgments

* SolarIOT @meltaxa
* sungrow2pvoutput @kronicd
* [pvoutput.org](https://pvoutput.org)

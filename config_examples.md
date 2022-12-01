
### Worker, Power Meter, HTTP
``` json
{
    "name": "worker",
    "address": "worker.address.com",
    "cpuThresh": "100",
    "powerThresh": "100",
    "cores": "24",
    "dynamicRange": [
        70,
        160
    ],
    "rpcServer": false,
    "httpPort": ":8080",
    "wattsup": {
        "path": "./wattslog",
        "cmd": "./wattsup ttyUSB0 -g watts"
    }
}

```

### Worker, No Power Meter, RCP
``` json
{
    "name": "worker",
    "address": "worker.address.com",
    "cpuThresh": "100",
    "powerThresh": "100",
    "cores": "24",
    "dynamicRange": [
        70,
        160
    ],
    "rpcServer": true,
    "rpcPort": ":3501,
}

```
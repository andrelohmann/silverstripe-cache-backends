# silverstripe cache backends

## Maintainers

 * Andre Lohmann (Nickname: andrelohmann)
  <lohmann dot andre at googlemail dot com>

## Requirements

Silverstripe 3.2.*

## Introduction

This repository adds some additional Cache Backends (Redis, MongoDB) to the silverstripe core

## Installation

put the follwoing post install and post updates scripts into your composer.json

```

    "scripts": {
        "post-install-cmd": [
            "cp vendor/andrelohmann-silverstripe/cache-backends/* -r framework/"
        ],
        "post-update-cmd": [
            "cp vendor/andrelohmann-silverstripe/cache-backends/* -r framework/"
        ]
    },
```
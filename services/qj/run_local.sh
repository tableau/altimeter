#!/bin/bash

export PYTHONPATH=../..
uvicorn main:app --reload

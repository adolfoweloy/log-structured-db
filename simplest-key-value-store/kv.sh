#!/bin/bash

dbset() {
  echo "$1,$2" >> database
}

dbget() {
  grep "^$1," database | sed "s/$1,//" | tail -n 1
}

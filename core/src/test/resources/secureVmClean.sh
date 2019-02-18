#!/bin/sh
# Cleaning shared memory leftovers
rm -rf /dev/shm/sgx-lkl-shmem*
ipcrm --all=shm

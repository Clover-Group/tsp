# Functional Testing User Guide

#### This folder contains directed functional tests, which stress Clover Group TSP engine functionality in a desired predefined way. All tests in this folder are expected to return a value, so make sure you get a valid data in the sink table after running each test.
 
### About 
#### Each file in this folder contains a REST message body with a request to TSP, which is expected to return some output data.

### Contents
#### rule135.txt - TBD
#### req_entity_33.txt - TBD
#### req_entity_1147.txt - TBD

### How to run the test

#### 1. Checkout the latest version of TSP
```bash
git clone https://github.com/Clover-Group/tsp -b v2core-refactoring 
```
#### 2. Run the TSP engine
```bash
cd ./tsp/runtime
ln -s docker-compose.yml.dev docker-compose.yml
./start.sh
```
#### 3. Clear the sink table ( carefully! )
```sql
delete * from events_2te116u_test_tsp
```
#### 4. Run the test 
```bash 
cd ./tsp/test/functional
../regression/tester.py <filename> 
```
#### 5. Validate output in the sink database
```sql
select * from events_2te116u_test_tsp
```

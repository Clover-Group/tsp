# This script implements a regression testing the the REST API
# It sends several requests in parallel threads to the TSP backend, checks responses and measures the execution timeouts
# Designed by Nikita Novikov
# Clover Group, Feb 21 2019

#!/usr/bin/python3

import json, time

import requests
import threading

# Flint server. Localhost or production
URL = 'http://localhost:8080/streamJob/from-jdbc/to-jdbc/?run_async=0'

# Paste full request here
REQS = [
{
	"sink": {
		"jdbcUrl": "jdbc:postgresql://192.168.40.101:30543/events?stringtype=unspecified",
		"password": "clover",
		"userName": "clover",
		"rowSchema": {
			"toTsField": "to_ts",
			"fromTsField": "from_ts",
			"contextField": "context",
			"appIdFieldVal": [
				"type",
				1
			],
			"sourceIdField": "series_storage",
			"patternIdField": "entity_id",
			"forwardedFields": [
				"loco_num",
				"Section",
				"upload_id"
			],
			"processingTsField": "processing_ts"
		},
		"tableName": "events_23ec4k",
		"driverName": "org.postgresql.Driver",
		"parallelism": 1,
		"batchInterval": 5000
	},
	"uuid": "44ffb062-ca1c-42ab-864c-b1b94eccb1fb",
	"source": {
		"query": "SELECT ts, loco_num AS loco_num, section AS \"Section\", upload_id AS upload_id, k4, k43con, k27, k8, u2, k30, k36con, km23con, ka21, k5con, k13con, km19con, s101, k7, k53, k23con, k24con, k30con, km33con, uks, k35con, km67con, k22, km66, k14con, miv_u1, km43con, km65con, k41, i3_4, ka1, km65, k17, k25, k20con, k26, km53, s102, k36, s20_venti, iab_z, bp12, k32, k6con, u1, k47con, km1, v2, bp11, k19con, k11con, k32con, k2con, v4, k21con, k41con, fpsn_ain4bug, k23, k10con, q21avar, im21, im12, k15, s20_tok1, qf21, ib1_2, k44con, k31con, v3, k3con, k24, k42con, s20_tok2, i1_2, km17con, k33con, km53con, s20_bv, npf, s73, km1con, s76, k15con, km66con, npz, k18con, s74, k53con, k34, opnpz, k25con, opnpf, s104, k29con, k35, k31, s75, k42, k1con, fpsn_ain2bug, km17, k18, k34con, k27con, k61con, k17con, k9, k52con, qf11, km69con, k62, km23, tormoz, qf1, im22, k10, k4con, k1, k20, miv_u2, k51con, k16, k11, k43, k5, k28, k26con, k13, k14, k2, k51, ib3_4, k16con, k54, k9con, sxsobr, k28con, v1, im11, q11avar, k62con, km69, s20_vozb, k61, k33, km15con, s20_avt, k6, km68, km15, km68con, k12con, k7con, k21, k47, k52, u3, km19, u4, km67, k8con, km43, k44, k22con, ka11, km16, s103, k19, k54con, km33, k29, k12, k3, km16con \nFROM es4k_tmy_20180720_wide \nWHERE upload_id = '1697954' AND ts >= 1548979200.0 AND ts < 1551398400.0 ORDER BY ts",
		"jdbcUrl": "jdbc:clickhouse://192.168.40.42:8123/default?user=loco_user&password=A7qoFxdH8PadW5B",
		"password": "A7qoFxdH8PadW5B",
		"sourceId": 127,
		"userName": "loco_user",
		"driverName": "ru.yandex.clickhouse.ClickHouseDriver",
		"parallelism": 1,
		"datetimeField": "ts",
		"eventsMaxGapMs": 60000,
		"partitionFields": [
			"loco_num",
			"Section",
			"upload_id"
		],
		"defaultEventsGapMs": 2000,
		"numParallelSources": 1,
		"patternsParallelism": 1
	},
	"patterns": [
		{
			"id": "1784",
			"payload": {},
			"sourceCode": "miv_u1 > 999 and s73 = 0 and s74 = 0 and s75 = 0 and npf >= 0 and uks >= 0 and v2 >= 0 and i1_2 >= 0 and i3_4 >= 0 and s20_vozb >= 0 and km23 < 2 and km43 < 2 and km23con < 2 and km43con < 2 and fpsn_ain2bug < 2 for  1 min ",
			"forwardedFields": []
		},
		{
			"id": "1790",
			"payload": {},
			"sourceCode": "(s101 = 1 andThen s103 = 1 or s104 = 1 for  3 sec > 0 times) or (s102 = 1 andThen s104 = 1 or s101 = 1 for  3 sec > 0 times) or (s103 = 1 andThen s101 = 1 or s102 = 1 for  3 sec > 0 times) or (s104 = 1 andThen s103 = 1 or s102 = 1 for  3 sec > 0 times)",
			"forwardedFields": []
		},
		{
			"id": "1853",
			"payload": {},
			"sourceCode": "sxsobr = 1 and uks > 2100 and s20_bv = 1 and s73 = 0 and s74 = 0 and s75 = 0 and s76 = 0 and qf1 = 1 and ka1 = 1 and (s20_tok2 = 1 or s20_tok1 = 1) andThen (s20_tok2 = 1 or s20_tok1 = 1) and s20_bv = 1 and (qf1 = 0 and ka1 = 0 for  1 sec > 0 times)",
			"forwardedFields": []
		},
		{
			"id": "1862",
			"payload": {},
			"sourceCode": "sxsobr = 1 and uks > 500 and s20_bv = 1 and s73 = 0 and s74 = 0 and s75 = 0 and s76 = 0 and qf1 = 1 and qf11 = 1 andThen qf1 = 0 and qf11 = 0 and npf >= 0 and npz >= 0 and opnpf >= 0 and opnpz >= 0 and v2 >= 0 and qf21 < 2 and s20_avt < 2 and s20_vozb < 2 and i1_2 >= 0 and i3_4 >= 0 and ib1_2 >= 0 and ib3_4 >= 0 for  1 sec > 0 times",
			"forwardedFields": []
		},
		{
			"id": "1873",
			"payload": {},
			"sourceCode": "v2 - avgOf(v1, v2, v3, v4) > 10",
			"forwardedFields": []
		},
		{
			"id": "1877",
			"payload": {},
			"sourceCode": "uks > 4300 and uks < 4800 for  0.5 sec ",
			"forwardedFields": []
		},
		{
			"id": "1887",
			"payload": {},
			"sourceCode": "k1 != k1con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1890",
			"payload": {},
			"sourceCode": "k4 != k4con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1893",
			"payload": {},
			"sourceCode": "k7 != k7con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1896",
			"payload": {},
			"sourceCode": "k10 != k10con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1899",
			"payload": {},
			"sourceCode": "k13 != k13con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1902",
			"payload": {},
			"sourceCode": "k16 != k16con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1905",
			"payload": {},
			"sourceCode": "k19 != k19con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1908",
			"payload": {},
			"sourceCode": "k22 != k22con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1911",
			"payload": {},
			"sourceCode": "k25 != k25con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1914",
			"payload": {},
			"sourceCode": "k28 != k28con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1917",
			"payload": {},
			"sourceCode": "k31 != k31con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1920",
			"payload": {},
			"sourceCode": "k34 != k34con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1923",
			"payload": {},
			"sourceCode": "k43 != k43con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1926",
			"payload": {},
			"sourceCode": "k51 != k51con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1929",
			"payload": {},
			"sourceCode": "k54 != k54con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2044",
			"payload": {},
			"sourceCode": "uks > 500 and (avgOf(v1, v2, v3, v4) - v1 > 10) and tormoz = 1 for  10 sec ",
			"forwardedFields": []
		},
		{
			"id": "2267",
			"payload": {},
			"sourceCode": "uks > 500 and (avgOf(v1, v2, v3, v4) - v3 > 10) and tormoz = 1 for  10 sec ",
			"forwardedFields": []
		},
		{
			"id": "2272",
			"payload": {},
			"sourceCode": "sxsobr = 1 and uks < 2100 and s20_bv = 1 and s73 = 0 and s74 = 0 and s75 = 0 and s76 = 0 and qf1 = 1 and ka1 = 1 andThen qf1 = 0 and ka1 = 0 for  1 sec > 0 times",
			"forwardedFields": []
		},
		{
			"id": "2275",
			"payload": {},
			"sourceCode": "npf = 1 and tormoz = 1 and i3_4 = 0 and im11 = 0 and im12 = 0 and i1_2 > 0 and im21 > 0 and im22 > 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2280",
			"payload": {},
			"sourceCode": "((u1 + u2) / 2 - u3) > 300 and q21avar = 0 andThen u3 = 0 for  3 sec ",
			"forwardedFields": []
		},
		{
			"id": "2283",
			"payload": {},
			"sourceCode": "km15 != km15con for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2286",
			"payload": {},
			"sourceCode": "km19 != km19con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2289",
			"payload": {},
			"sourceCode": "km53 != km53con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2292",
			"payload": {},
			"sourceCode": "km67 != km67con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2295",
			"payload": {},
			"sourceCode": "km33 != km33con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2298",
			"payload": {},
			"sourceCode": "k3 = 1 and k3con = 1 andThen k3 = 1 and k3con = 0 andThen k3 = 1 and k3con = 1",
			"forwardedFields": []
		},
		{
			"id": "2301",
			"payload": {},
			"sourceCode": "k6 = 1 and k6con = 1 andThen k6 = 1 and k6con = 0 andThen k6 = 1 and k6con = 1",
			"forwardedFields": []
		},
		{
			"id": "2304",
			"payload": {},
			"sourceCode": "k9 = 1 and k9con = 1 andThen k9 = 1 and k9con = 0 andThen k9 = 1 and k9con = 1",
			"forwardedFields": []
		},
		{
			"id": "2307",
			"payload": {},
			"sourceCode": "k10 = 1 and k10con = 1 andThen k10 = 1 and k10con = 0 andThen k10 = 1 and k10con = 1",
			"forwardedFields": []
		},
		{
			"id": "2310",
			"payload": {},
			"sourceCode": "k12 = 1 and k12con = 1 andThen k12 = 1 and k12con = 0 andThen k12 = 1 and k12con = 1",
			"forwardedFields": []
		},
		{
			"id": "2313",
			"payload": {},
			"sourceCode": "k15 = 1 and k15con = 1 andThen k15 = 1 and k15con = 0 andThen k15 = 1 and k15con = 1",
			"forwardedFields": []
		},
		{
			"id": "2316",
			"payload": {},
			"sourceCode": "k1 = 18 and k1con = 18 andThen k18 = 1 and k18con = 0 andThen k18 = 1 and k18con = 1",
			"forwardedFields": []
		},
		{
			"id": "2319",
			"payload": {},
			"sourceCode": "k21 = 1 and k21con = 1 andThen k21 = 1 and k21con = 0 andThen k21 = 1 and k21con = 1",
			"forwardedFields": []
		},
		{
			"id": "2322",
			"payload": {},
			"sourceCode": "k24 = 1 and k24con = 1 andThen k24 = 1 and k24con = 0 andThen k24 = 1 and k24con = 1",
			"forwardedFields": []
		},
		{
			"id": "2325",
			"payload": {},
			"sourceCode": "k28 = 1 and k28con = 1 andThen k28 = 1 and k28con = 0 andThen k28 = 1 and k28con = 1",
			"forwardedFields": []
		},
		{
			"id": "2328",
			"payload": {},
			"sourceCode": "k30 = 1 and k30con = 1 andThen k30 = 1 and k30con = 0 andThen k30 = 1 and k30con = 1",
			"forwardedFields": []
		},
		{
			"id": "2331",
			"payload": {},
			"sourceCode": "k33 = 1 and k33con = 1 andThen k33 = 1 and k33con = 0 andThen k33 = 1 and k33con = 1",
			"forwardedFields": []
		},
		{
			"id": "2334",
			"payload": {},
			"sourceCode": "k36 = 1 and k36con = 1 andThen k36 = 1 and k36con = 0 andThen k36 = 1 and k36con = 1",
			"forwardedFields": []
		},
		{
			"id": "2337",
			"payload": {},
			"sourceCode": "k42 = 1 and k42con = 1 andThen k42 = 1 and k42con = 0 andThen k42 = 1 and k42con = 1",
			"forwardedFields": []
		},
		{
			"id": "2340",
			"payload": {},
			"sourceCode": "km33 = 1 and km33con = 1 andThen km33 = 1 and km33con = 0 andThen km33 = 1 and km33con = 1",
			"forwardedFields": []
		},
		{
			"id": "2343",
			"payload": {},
			"sourceCode": "k47 = 1 and k47con = 1 andThen k47 = 1 and k47con = 0 andThen k47 = 1 and k47con = 1",
			"forwardedFields": []
		},
		{
			"id": "2346",
			"payload": {},
			"sourceCode": "km67 = 1 and km67con = 1 andThen km67 = 1 and km67con = 0 andThen km67 = 1 and km67con = 1",
			"forwardedFields": []
		},
		{
			"id": "2349",
			"payload": {},
			"sourceCode": "k53 = 1 and k53con = 1 andThen k53 = 1 and k53con = 0 andThen k53 = 1 and k53con = 1",
			"forwardedFields": []
		},
		{
			"id": "2352",
			"payload": {},
			"sourceCode": "k54 = 1 and k54con = 1 andThen k54 = 1 and k54con = 0 andThen k54 = 1 and k54con = 1",
			"forwardedFields": []
		},
		{
			"id": "2355",
			"payload": {},
			"sourceCode": "k61 = 1 and k61con = 1 andThen k61 = 1 and k61con = 0 andThen k61 = 1 and k61con = 1",
			"forwardedFields": []
		},
		{
			"id": "2358",
			"payload": {},
			"sourceCode": "km17 = 1 and km17con = 1 andThen km17 = 1 and km17con = 0 andThen km17 = 1 and km17con = 1",
			"forwardedFields": []
		},
		{
			"id": "2361",
			"payload": {},
			"sourceCode": "km1 = 1 and km1con = 1 andThen km1 = 1 and km1con = 0 andThen km1 = 1 and km1con = 1",
			"forwardedFields": []
		}
	]
},
######################
{
	"sink": {
		"jdbcUrl": "jdbc:postgresql://192.168.40.101:30543/events?stringtype=unspecified",
		"password": "clover",
		"userName": "clover",
		"rowSchema": {
			"toTsField": "to_ts",
			"fromTsField": "from_ts",
			"contextField": "context",
			"appIdFieldVal": [
				"type",
				1
			],
			"sourceIdField": "series_storage",
			"patternIdField": "entity_id",
			"forwardedFields": [
				"loco_num",
				"Section",
				"upload_id"
			],
			"processingTsField": "processing_ts"
		},
		"tableName": "events_23ec4k",
		"driverName": "org.postgresql.Driver",
		"parallelism": 1,
		"batchInterval": 5000
	},
	"uuid": "44ffb062-ca1c-42ab-864c-b1b94eccb1fb",
	"source": {
		"query": "SELECT ts, loco_num AS loco_num, section AS \"Section\", upload_id AS upload_id, k4, k43con, k27, k8, u2, k30, k36con, km23con, ka21, k5con, k13con, km19con, s101, k7, k53, k23con, k24con, k30con, km33con, uks, k35con, km67con, k22, km66, k14con, miv_u1, km43con, km65con, k41, i3_4, ka1, km65, k17, k25, k20con, k26, km53, s102, k36, s20_venti, iab_z, bp12, k32, k6con, u1, k47con, km1, v2, bp11, k19con, k11con, k32con, k2con, v4, k21con, k41con, fpsn_ain4bug, k23, k10con, q21avar, im21, im12, k15, s20_tok1, qf21, ib1_2, k44con, k31con, v3, k3con, k24, k42con, s20_tok2, i1_2, km17con, k33con, km53con, s20_bv, npf, s73, km1con, s76, k15con, km66con, npz, k18con, s74, k53con, k34, opnpz, k25con, opnpf, s104, k29con, k35, k31, s75, k42, k1con, fpsn_ain2bug, km17, k18, k34con, k27con, k61con, k17con, k9, k52con, qf11, km69con, k62, km23, tormoz, qf1, im22, k10, k4con, k1, k20, miv_u2, k51con, k16, k11, k43, k5, k28, k26con, k13, k14, k2, k51, ib3_4, k16con, k54, k9con, sxsobr, k28con, v1, im11, q11avar, k62con, km69, s20_vozb, k61, k33, km15con, s20_avt, k6, km68, km15, km68con, k12con, k7con, k21, k47, k52, u3, km19, u4, km67, k8con, km43, k44, k22con, ka11, km16, s103, k19, k54con, km33, k29, k12, k3, km16con \nFROM es4k_tmy_20180720_wide \nWHERE upload_id = '1697954' AND ts >= 1548979200.0 AND ts < 1551398400.0 ORDER BY ts",
		"jdbcUrl": "jdbc:clickhouse://192.168.40.42:8123/default?user=loco_user&password=A7qoFxdH8PadW5B",
		"password": "A7qoFxdH8PadW5B",
		"sourceId": 127,
		"userName": "loco_user",
		"driverName": "ru.yandex.clickhouse.ClickHouseDriver",
		"parallelism": 1,
		"datetimeField": "ts",
		"eventsMaxGapMs": 60000,
		"partitionFields": [
			"loco_num",
			"Section",
			"upload_id"
		],
		"defaultEventsGapMs": 2000,
		"numParallelSources": 1,
		"patternsParallelism": 1
	},
	"patterns": [
		{
			"id": "1786",
			"payload": {},
			"sourceCode": "uks > 500 and (v1 > 10 or v2 > 10 or v3 > 10 or v4 > 10) and npz = 0 and s20_venti = 0 for  120 sec ",
			"forwardedFields": []
		},
		{
			"id": "1792",
			"payload": {},
			"sourceCode": "uks > 2000 and (v1 > 2 or v2 > 2 or v3 > 2 or v4 > 2) and (i1_2 > 10 or i3_4 > 10) and (bp11 > 0.2 or bp12 > 0.2) and iab_z > 0 for  5 sec ",
			"forwardedFields": []
		},
		{
			"id": "1857",
			"payload": {},
			"sourceCode": "sxsobr = 1 and uks > 2100 and s20_bv = 1 and s73 = 0 and s74 = 0 and s75 = 0 and s76 = 0 and qf1 = 1 and ka11 = 1 and ka21 = 1 andThen qf1 = 0 and ka11 = 0 and ka21 = 0 for  1 sec > 0 times",
			"forwardedFields": []
		},
		{
			"id": "1863",
			"payload": {},
			"sourceCode": "sxsobr = 1 and uks > 500 and s20_bv = 1 and s73 = 0 and s74 = 0 and s75 = 0 and s76 = 0 and qf1 = 1 and qf21 = 1 andThen qf1 = 0 and qf21 = 0 and npf >= 0 and npz >= 0 and opnpf >= 0 and opnpz >= 0 and v2 >= 0 and qf21 < 2 and s20_avt < 2 and s20_vozb < 2 and i1_2 >= 0 and i3_4 >= 0 and ib1_2 >= 0 and ib3_4 >= 0 for  1 sec > 0 times",
			"forwardedFields": []
		},
		{
			"id": "1874",
			"payload": {},
			"sourceCode": "v3 - avgOf(v1, v2, v3, v4) > 10",
			"forwardedFields": []
		},
		{
			"id": "1884",
			"payload": {},
			"sourceCode": "(npz = 31 and npf = 32 and v3 < 27 and v2 < 27) or (npz = 16 and npf = 17 and v3 < 9 and v2 < 9)",
			"forwardedFields": []
		},
		{
			"id": "1888",
			"payload": {},
			"sourceCode": "k2 != k2con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1891",
			"payload": {},
			"sourceCode": "k5 != k5con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1894",
			"payload": {},
			"sourceCode": "k8 != k8con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1897",
			"payload": {},
			"sourceCode": "k11 != k11con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1900",
			"payload": {},
			"sourceCode": "k14 != k14con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1903",
			"payload": {},
			"sourceCode": "k17 != k17con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1906",
			"payload": {},
			"sourceCode": "k20 != k20con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1909",
			"payload": {},
			"sourceCode": "k23 != k23con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1912",
			"payload": {},
			"sourceCode": "k26 != k26con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1915",
			"payload": {},
			"sourceCode": "k29 != k29con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1918",
			"payload": {},
			"sourceCode": "k32 != k32con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1921",
			"payload": {},
			"sourceCode": "k41 != k41con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1924",
			"payload": {},
			"sourceCode": "k44 != k44con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1927",
			"payload": {},
			"sourceCode": "k52 != k52con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1930",
			"payload": {},
			"sourceCode": "k61 != k61con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2265",
			"payload": {},
			"sourceCode": "miv_u2 > 999 and s73 = 0 and s74 = 0 and s75 = 0 and npf >= 0 and uks >= 0 and v2 >= 0 and i1_2 >= 0 and i3_4 >= 0 and s20_vozb >= 0 and km23 < 2 and km43 < 2 and km23con < 2 and km43con < 2 and fpsn_ain4bug < 2 for  1 min ",
			"forwardedFields": []
		},
		{
			"id": "2268",
			"payload": {},
			"sourceCode": "uks > 500 and (avgOf(v1, v2, v3, v4) - v4 > 10) and tormoz = 1 for  10 sec ",
			"forwardedFields": []
		},
		{
			"id": "2273",
			"payload": {},
			"sourceCode": "sxsobr = 1 and uks > 2100 and s20_bv = 1 and s73 = 0 and s74 = 0 and s75 = 0 and s76 = 0 and qf1 = 1 and ka11 = 1 and ka21 = 1 andThen ka21 = 1 and (qf1 = 0 and ka11 = 0 for  1 sec > 0 times)",
			"forwardedFields": []
		},
		{
			"id": "2277",
			"payload": {},
			"sourceCode": "((u3 + u4) / 2 - u1) > 300 and q11avar = 0 andThen u1 = 0 for  3 sec ",
			"forwardedFields": []
		},
		{
			"id": "2281",
			"payload": {},
			"sourceCode": "((u1 + u2) / 2 - u4) > 300 and q21avar = 0 andThen u4 = 0 for  3 sec ",
			"forwardedFields": []
		},
		{
			"id": "2284",
			"payload": {},
			"sourceCode": "km16 != km16con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2287",
			"payload": {},
			"sourceCode": "km23 != km23con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2290",
			"payload": {},
			"sourceCode": "km65 != km65con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2293",
			"payload": {},
			"sourceCode": "km68 != km68con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2296",
			"payload": {},
			"sourceCode": "k1 = 1 and k1con = 1 andThen k1 = 1 and k1con = 0 andThen k1 = 1 and k1con = 1",
			"forwardedFields": []
		},
		{
			"id": "2299",
			"payload": {},
			"sourceCode": "k4 = 1 and k4con = 1 andThen k4 = 1 and k4con = 0 andThen k4 = 1 and k4con = 1",
			"forwardedFields": []
		},
		{
			"id": "2302",
			"payload": {},
			"sourceCode": "k7 = 1 and k7con = 1 andThen k7 = 1 and k7con = 0 andThen k7 = 1 and k7con = 1",
			"forwardedFields": []
		},
		{
			"id": "2305",
			"payload": {},
			"sourceCode": "k10 = 1 and k10con = 1 andThen k10 = 1 and k10con = 0 andThen k10 = 1 and k10con = 1",
			"forwardedFields": []
		},
		{
			"id": "2308",
			"payload": {},
			"sourceCode": "k10 = 1 and k10con = 1 andThen k10 = 1 and k10con = 0 andThen k10 = 1 and k10con = 1",
			"forwardedFields": []
		},
		{
			"id": "2311",
			"payload": {},
			"sourceCode": "k13 = 1 and k13con = 1 andThen k13 = 1 and k13con = 0 andThen k13 = 1 and k13con = 1",
			"forwardedFields": []
		},
		{
			"id": "2314",
			"payload": {},
			"sourceCode": "k16 = 1 and k16con = 1 andThen k16 = 1 and k16con = 0 andThen k16 = 1 and k16con = 1",
			"forwardedFields": []
		},
		{
			"id": "2317",
			"payload": {},
			"sourceCode": "k19 = 1 and k19con = 1 andThen k19 = 1 and k19con = 0 andThen k19 = 1 and k19con = 1",
			"forwardedFields": []
		},
		{
			"id": "2320",
			"payload": {},
			"sourceCode": "k22 = 1 and k22con = 1 andThen k22 = 1 and k22con = 0 andThen k22 = 1 and k22con = 1",
			"forwardedFields": []
		},
		{
			"id": "2323",
			"payload": {},
			"sourceCode": "k25 = 1 and k25con = 1 andThen k25 = 1 and k25con = 0 andThen k25 = 1 and k25con = 1",
			"forwardedFields": []
		},
		{
			"id": "2326",
			"payload": {},
			"sourceCode": "k28 = 1 and k28con = 1 andThen k28 = 1 and k28con = 0 andThen k28 = 1 and k28con = 1",
			"forwardedFields": []
		},
		{
			"id": "2329",
			"payload": {},
			"sourceCode": "k31 = 1 and k31con = 1 andThen k31 = 1 and k31con = 0 andThen k31 = 1 and k31con = 1",
			"forwardedFields": []
		},
		{
			"id": "2332",
			"payload": {},
			"sourceCode": "k34 = 1 and k34con = 1 andThen k34 = 1 and k34con = 0 andThen k34 = 1 and k34con = 1",
			"forwardedFields": []
		},
		{
			"id": "2335",
			"payload": {},
			"sourceCode": "k41 = 1 and k41con = 1 andThen k41 = 1 and k41con = 0 andThen k41 = 1 and k41con = 1",
			"forwardedFields": []
		},
		{
			"id": "2338",
			"payload": {},
			"sourceCode": "k35 != k35con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2341",
			"payload": {},
			"sourceCode": "k44 = 1 and k44con = 1 andThen k44 = 1 and k44con = 0 andThen k44 = 1 and k44con = 1",
			"forwardedFields": []
		},
		{
			"id": "2344",
			"payload": {},
			"sourceCode": "km68 = 1 and km68con = 1 andThen km68 = 1 and km68con = 0 andThen km68 = 1 and km68con = 1",
			"forwardedFields": []
		},
		{
			"id": "2347",
			"payload": {},
			"sourceCode": "km66 = 1 and km66con = 1 andThen km66 = 1 and km66con = 0 andThen km66 = 1 and km66con = 1",
			"forwardedFields": []
		},
		{
			"id": "2350",
			"payload": {},
			"sourceCode": "km65 = 1 and km65con = 1 andThen km65 = 1 and km65con = 0 andThen km65 = 1 and km65con = 1",
			"forwardedFields": []
		},
		{
			"id": "2353",
			"payload": {},
			"sourceCode": "km33 = 1 and km33con = 1 andThen km33 = 1 and km33con = 0 andThen km33 = 1 and km33con = 1",
			"forwardedFields": []
		},
		{
			"id": "2356",
			"payload": {},
			"sourceCode": "km19 = 1 and km19con = 1 andThen km19 = 1 and km19con = 0 andThen km19 = 1 and km19con = 1",
			"forwardedFields": []
		},
		{
			"id": "2359",
			"payload": {},
			"sourceCode": "km16 = 1 and km16con = 1 andThen km16 = 1 and km16con = 0 andThen km16 = 1 and km16con = 1",
			"forwardedFields": []
		}
	]
},
######################
{
	"sink": {
		"jdbcUrl": "jdbc:postgresql://192.168.40.101:30543/events?stringtype=unspecified",
		"password": "clover",
		"userName": "clover",
		"rowSchema": {
			"toTsField": "to_ts",
			"fromTsField": "from_ts",
			"contextField": "context",
			"appIdFieldVal": [
				"type",
				1
			],
			"sourceIdField": "series_storage",
			"patternIdField": "entity_id",
			"forwardedFields": [
				"loco_num",
				"Section",
				"upload_id"
			],
			"processingTsField": "processing_ts"
		},
		"tableName": "events_23ec4k",
		"driverName": "org.postgresql.Driver",
		"parallelism": 1,
		"batchInterval": 5000
	},
	"uuid": "44ffb062-ca1c-42ab-864c-b1b94eccb1fb",
	"source": {
		"query": "SELECT ts, loco_num AS loco_num, section AS \"Section\", upload_id AS upload_id, k4, k43con, k27, k8, u2, k30, k36con, km23con, ka21, k5con, k13con, km19con, s101, k7, k53, k23con, k24con, k30con, km33con, uks, k35con, km67con, k22, km66, k14con, miv_u1, km43con, km65con, k41, i3_4, ka1, km65, k17, k25, k20con, k26, km53, s102, k36, s20_venti, iab_z, bp12, k32, k6con, u1, k47con, km1, v2, bp11, k19con, k11con, k32con, k2con, v4, k21con, k41con, fpsn_ain4bug, k23, k10con, q21avar, im21, im12, k15, s20_tok1, qf21, ib1_2, k44con, k31con, v3, k3con, k24, k42con, s20_tok2, i1_2, km17con, k33con, km53con, s20_bv, npf, s73, km1con, s76, k15con, km66con, npz, k18con, s74, k53con, k34, opnpz, k25con, opnpf, s104, k29con, k35, k31, s75, k42, k1con, fpsn_ain2bug, km17, k18, k34con, k27con, k61con, k17con, k9, k52con, qf11, km69con, k62, km23, tormoz, qf1, im22, k10, k4con, k1, k20, miv_u2, k51con, k16, k11, k43, k5, k28, k26con, k13, k14, k2, k51, ib3_4, k16con, k54, k9con, sxsobr, k28con, v1, im11, q11avar, k62con, km69, s20_vozb, k61, k33, km15con, s20_avt, k6, km68, km15, km68con, k12con, k7con, k21, k47, k52, u3, km19, u4, km67, k8con, km43, k44, k22con, ka11, km16, s103, k19, k54con, km33, k29, k12, k3, km16con \nFROM es4k_tmy_20180720_wide \nWHERE upload_id = '1697954' AND ts >= 1548979200.0 AND ts < 1551398400.0 ORDER BY ts",
		"jdbcUrl": "jdbc:clickhouse://192.168.40.42:8123/default?user=loco_user&password=A7qoFxdH8PadW5B",
		"password": "A7qoFxdH8PadW5B",
		"sourceId": 127,
		"userName": "loco_user",
		"driverName": "ru.yandex.clickhouse.ClickHouseDriver",
		"parallelism": 1,
		"datetimeField": "ts",
		"eventsMaxGapMs": 60000,
		"partitionFields": [
			"loco_num",
			"Section",
			"upload_id"
		],
		"defaultEventsGapMs": 2000,
		"numParallelSources": 1,
		"patternsParallelism": 1
	},
	"patterns": [
		{
			"id": "1788",
			"payload": {},
			"sourceCode": "uks > 500 and (((v1 - (v1 + v2 + v3 + v4) / 4) > 20) or ((v2 - (v1 + v2 + v3 + v4) / 4 - v2) > 20) or ((v3 - (v1 + v2 + v3 + v4) / 4) > 20) or ((v1 - (v1 + v2 + v3 + v4) / 4) > 20)) and npz >= lag(npz) for  5 sec ",
			"forwardedFields": []
		},
		{
			"id": "1850",
			"payload": {},
			"sourceCode": "uks > 500 and npz > 0 and (i1_2 > 850 or ib1_2 > 850 or i3_4 > 850 or ib3_4 > 850)",
			"forwardedFields": []
		},
		{
			"id": "1861",
			"payload": {},
			"sourceCode": "sxsobr = 1 and uks > 500 and s20_bv = 1 and s73 = 0 and s74 = 0 and s75 = 0 and s76 = 0 and qf1 = 1 and ka1 = 1 and ka11 = 1 and ka21 = 1 and qf11 = 1 and qf21 = 1 andThen ka1 = 1 and ka11 = 1 and ka21 = 1 and qf11 = 1 and qf21 = 1 and (qf1 = 0 for  1 sec > 0 times)",
			"forwardedFields": []
		},
		{
			"id": "1872",
			"payload": {},
			"sourceCode": "v1 - avgOf(v1, v2, v3, v4) > 10",
			"forwardedFields": []
		},
		{
			"id": "1875",
			"payload": {},
			"sourceCode": "v4 - avgOf(v1, v2, v3, v4) > 10",
			"forwardedFields": []
		},
		{
			"id": "1886",
			"payload": {},
			"sourceCode": "k1 != k1con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1889",
			"payload": {},
			"sourceCode": "k3 != k3con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1892",
			"payload": {},
			"sourceCode": "k6 != k6con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1895",
			"payload": {},
			"sourceCode": "k9 != k9con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1898",
			"payload": {},
			"sourceCode": "k12 != k12con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1901",
			"payload": {},
			"sourceCode": "k15 != k15con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1904",
			"payload": {},
			"sourceCode": "k18 != k18con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1907",
			"payload": {},
			"sourceCode": "k21 != k21con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1910",
			"payload": {},
			"sourceCode": "k24 != k24con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1913",
			"payload": {},
			"sourceCode": "k27 != k27con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1916",
			"payload": {},
			"sourceCode": "k30 != k30con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1919",
			"payload": {},
			"sourceCode": "k33 != k33con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1922",
			"payload": {},
			"sourceCode": "k42 != k42con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1925",
			"payload": {},
			"sourceCode": "k47 != k47con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1928",
			"payload": {},
			"sourceCode": "k53 != k53con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "1931",
			"payload": {},
			"sourceCode": "k62 != k62con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2266",
			"payload": {},
			"sourceCode": "uks > 500 and (avgOf(v1, v2, v3, v4) - v2 > 10) and tormoz = 1 for  10 sec ",
			"forwardedFields": []
		},
		{
			"id": "2271",
			"payload": {},
			"sourceCode": "uks > 4800 for  0.5 sec ",
			"forwardedFields": []
		},
		{
			"id": "2274",
			"payload": {},
			"sourceCode": "sxsobr = 1 and uks > 2100 and s20_bv = 1 and s73 = 0 and s74 = 0 and s75 = 0 and s76 = 0 and qf1 = 1 and ka11 = 1 and ka21 = 1 andThen ka11 = 1 and (qf1 = 0 and ka21 = 0 for  1 sec > 0 times)",
			"forwardedFields": []
		},
		{
			"id": "2279",
			"payload": {},
			"sourceCode": "((u3 + u4) / 2 - u2) > 300 and q11avar = 0 andThen u2 = 0 for  3 sec ",
			"forwardedFields": []
		},
		{
			"id": "2282",
			"payload": {},
			"sourceCode": "km1 != km1con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2285",
			"payload": {},
			"sourceCode": "km17 != km17con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2288",
			"payload": {},
			"sourceCode": "km43 != km43con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2291",
			"payload": {},
			"sourceCode": "km66 != km66con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2294",
			"payload": {},
			"sourceCode": "km69 != km69con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2297",
			"payload": {},
			"sourceCode": "k2 = 1 and k2con = 1 andThen k2 = 1 and k2con = 0 andThen k2 = 1 and k2con = 1",
			"forwardedFields": []
		},
		{
			"id": "2300",
			"payload": {},
			"sourceCode": "k5 = 1 and k5con = 1 andThen k5 = 1 and k5con = 0 andThen k5 = 1 and k5con = 1",
			"forwardedFields": []
		},
		{
			"id": "2303",
			"payload": {},
			"sourceCode": "k8 = 1 and k8con = 1 andThen k8 = 1 and k8con = 0 andThen k8 = 1 and k8con = 1",
			"forwardedFields": []
		},
		{
			"id": "2306",
			"payload": {},
			"sourceCode": "k10 = 1 and k10con = 1 andThen k10 = 1 and k10con = 0 andThen k10 = 1 and k10con = 1",
			"forwardedFields": []
		},
		{
			"id": "2309",
			"payload": {},
			"sourceCode": "k11 = 1 and k11con = 1 andThen k11 = 1 and k11con = 0 andThen k11 = 1 and k11con = 1",
			"forwardedFields": []
		},
		{
			"id": "2312",
			"payload": {},
			"sourceCode": "k1 = 14 and k14con = 1 andThen k14 = 1 and k14con = 0 andThen k14 = 1 and k14con = 1",
			"forwardedFields": []
		},
		{
			"id": "2315",
			"payload": {},
			"sourceCode": "k17 = 1 and k17con = 1 andThen k17 = 1 and k17con = 0 andThen k17 = 1 and k17con = 1",
			"forwardedFields": []
		},
		{
			"id": "2318",
			"payload": {},
			"sourceCode": "k20 = 1 and k20con = 1 andThen k20 = 1 and k20con = 0 andThen k20 = 1 and k20con = 1",
			"forwardedFields": []
		},
		{
			"id": "2321",
			"payload": {},
			"sourceCode": "k23 = 1 and k23con = 1 andThen k23 = 1 and k23con = 0 andThen k23 = 1 and k23con = 1",
			"forwardedFields": []
		},
		{
			"id": "2324",
			"payload": {},
			"sourceCode": "k26 = 1 and k26con = 1 andThen k26 = 1 and k26con = 0 andThen k26 = 1 and k26con = 1",
			"forwardedFields": []
		},
		{
			"id": "2327",
			"payload": {},
			"sourceCode": "k29 = 1 and k29con = 1 andThen k29 = 1 and k29con = 0 andThen k29 = 1 and k29con = 1",
			"forwardedFields": []
		},
		{
			"id": "2330",
			"payload": {},
			"sourceCode": "k32 = 1 and k32con = 1 andThen k32 = 1 and k32con = 0 andThen k32 = 1 and k32con = 1",
			"forwardedFields": []
		},
		{
			"id": "2333",
			"payload": {},
			"sourceCode": "k35 = 1 and k35con = 1 andThen k35 = 1 and k35con = 0 andThen k35 = 1 and k35con = 1",
			"forwardedFields": []
		},
		{
			"id": "2336",
			"payload": {},
			"sourceCode": "k36 != k36con and npz >= 0 and npf >= 0 and uks >= 0 and qf1 >= 0 and ka1 >= 0 and i1_2 >= 0 and i3_4 >= 0 and v1 >= 0 for  1 sec ",
			"forwardedFields": []
		},
		{
			"id": "2339",
			"payload": {},
			"sourceCode": "k43 = 1 and k43con = 1 andThen k43 = 1 and k43con = 0 andThen k43 = 1 and k43con = 1",
			"forwardedFields": []
		},
		{
			"id": "2342",
			"payload": {},
			"sourceCode": "km69 = 1 and km69con = 1 andThen km69 = 1 and km69con = 0 andThen km69 = 1 and km69con = 1",
			"forwardedFields": []
		},
		{
			"id": "2345",
			"payload": {},
			"sourceCode": "k51 = 1 and k51con = 1 andThen k51 = 1 and k51con = 0 andThen k51 = 1 and k51con = 1",
			"forwardedFields": []
		},
		{
			"id": "2348",
			"payload": {},
			"sourceCode": "k52 = 1 and k52con = 1 andThen k52 = 1 and k52con = 0 andThen k52 = 1 and k52con = 1",
			"forwardedFields": []
		},
		{
			"id": "2351",
			"payload": {},
			"sourceCode": "km43 = 1 and km43con = 1 andThen km43 = 1 and km43con = 0 andThen km43 = 1 and km43con = 1",
			"forwardedFields": []
		},
		{
			"id": "2354",
			"payload": {},
			"sourceCode": "km23 = 1 and km23con = 1 andThen km23 = 1 and km23con = 0 andThen km23 = 1 and km23con = 1",
			"forwardedFields": []
		},
		{
			"id": "2357",
			"payload": {},
			"sourceCode": "k62 = 1 and k62con = 1 andThen k62 = 1 and k62con = 0 andThen k62 = 1 and k62con = 1",
			"forwardedFields": []
		},
		{
			"id": "2360",
			"payload": {},
			"sourceCode": "km15 = 1 and km15con = 1 andThen km15 = 1 and km15con = 0 andThen km15 = 1 and km15con = 1",
			"forwardedFields": []
		}
	]
}

]

print(len(REQS))


def send(indx: int, req: dict, responses: list):
    print(indx, 'sended.')
    resp = requests.post(URL, json=req, headers={"Content-Type": "application/json"})
    #resp = requests.get(URL, headers={"Content-Type": "application/json"})
    print(indx, 'got response.')

    try:
        p_resp = resp.json()
    except json.JSONDecodeError:
        p_resp = str(resp)

    responses.append((indx, p_resp))


threads = []
responses = []

req_count = len(REQS)
for indx, r in enumerate(REQS[:req_count]):
        threads.append(threading.Thread(
        target=send, args=(indx, r, responses),
        daemon=True))

t1 = time.time()
for t in threads:
    t.start()

# thread sync
for t in threads:
    t.join()

#print(f'Time: {time.time() - t1:.3f}')
print('Time: {0}'.format(time.time() - t1))
print(responses)

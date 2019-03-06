# TSP top Makefile
# Boris V.Kuznetsov
# Clover Group
# Feb 23 2019

show:
	@echo
	@echo "**** TSP v2 Garphield build flow ****"
	@echo
	@echo "make lint  - run lint tool"
	@echo "make cov   - report coverage"
	@echo "make test  - run all scala tests"
	@echo "make regr  - run regression test"
	@echo "make clean - clean all generated data"
	
lint:
	@echo "Running Source Linting. Output is in ./target/scala-2.12/scapegoat-report"
	@sbt scapegoat

test:
	@echo "Running all Scala tests ..."
	@sbt test

cov:
	@echo "Running Coverage. Output is in */target/scala-2.12/scoverage-report"
	@sbt clean coverage test
	@sbt coverageReport 

regr:
	@echo "Running Regression Test ..."
	@python3.6 test/regression/tester.py test/reqs/* --batch-size 3

clean:
	@find . -name "target" | xargs rm -rf {} \;

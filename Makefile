# TSP top Makefile
# Boris V.Kuznetsov
# Clover Group
# Feb 23 2019

show:
	@echo
	@echo "**** TSP v2 Garphield build flow ****"
	@echo
	@echo "make lint  - run lint tool"
	@echo "make ccov  - report core coverage"
	@echo "make dcov  - report dsl coverage"
	@echo "make cov   - report all coverage"
	@echo "make test  - run all scala tests"
	@echo "make regr  - run regression test"
	@echo "make clean - clean all generated data"
	
lint:
	@echo "Running Source Linting. Output is in ./target/scala-2.12/scapegoat-report"
	@sbt scapegoat

test:
	@echo "Running all Scala tests ..."
	@sbt test

ccov:
	@echo "Running Core Coverage. Output is in core/target/scala-2.12/scoverage-report"
	@sbt clean coverage core/test
	@sbt coverageReport 

dcov:
	@echo "Running DSL Coverage. Output is in dsl/target/scala-2.12/scoverage-report"
	@sbt clean coverage dsl/test
	@sbt coverageReport 

cov:
	@echo "Running Coverage. Output is in */target/scala-2.12/scoverage-report"
	@sbt clean coverage test
	@sbt coverageReport 

regr:
	@echo "Running Regression Test ..."
	@python3.6 test/regression/tester.py test/reqs/* --batch-size 3

clean:
	@find . -name "target" | xargs rm -rf {} \;



kafka-up:
	@docker-compose up >> /dev/null

kafka-down:
	@docker-compose stop

mock-start:
	@${PWD}/.venv/bin/python ${PWD}/challenge_pismoio/mock_producer.py

mock-stop:
	@kill -9 $(ps aux | grep mock_producer.py | awk 'NR==1{print $2}')

event-process-start:
	@${PWD}/.venv/bin/python ${PWD}/challenge_pismoio/event_processor.py

event-process-stop:
	@kill -9 $(ps aux | grep event_processor.py | awk 'NR==1{print $2}')

debugger-start:
	@${PWD}/.venv/bin/python ${PWD}/challenge_pismoio/debugger.py

debugger-stop:
	@kill -9 $(ps aux | grep debugger.py | awk 'NR==1{print $2}')

test:
	@poetry run python -m pytest \
		--cov=. \
		--cov-report=term \
		--cov-report=html:coverage-report \
		challenge_pismoio/tests/
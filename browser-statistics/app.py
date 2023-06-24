import logging
import os
import faust
import pandas as pd

logging.basicConfig(level=logging.INFO)

HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
TOPIC = os.getenv("TOPIC")
kafka_broker = f'{HOST}:{PORT}'


class AggVisitRecord(faust.Record):
	defined_domain: str


app = faust.App('browser-statistics', broker=kafka_broker)
app.producer.buffer.max_messages = 10000

raw_topic = app.topic(f'{TOPIC}', key_type=str, value_type=str, value_serializer='raw', partitions=8)
visits_agg_topic = app.topic('visits_agg_topic', key_type=str, value_type=AggVisitRecord, partitions=8, internal=True)
visits = app.Table("visits", key_type=str, value_type=int, partitions=8, default=int)


@app.agent(raw_topic)
async def statistic_processing(messages):
	async for message in messages:
		print(f'{TOPIC}, {message}')
		logging.info("%d:%s message=%s", TOPIC, message)
		message_url = message.split(',')[1]
		parsed_url = message_url.split('/')
		message_domain = [
			parsed_url[2].split('.')[-1] if (parsed_url[0] == 'https:' or parsed_url[0] == 'http:') else parsed_url]
		await visits_agg_topic.send(value=AggVisitRecord(
			defined_domain=message_domain))


@app.agent(visits_agg_topic)
async def aggregate_data(stream):
	async for event in stream:
		visit = event.defined_domain
		visits[str(visit)] += 1
		print(visits)


def create_df(col_name):
	tables = {"Visits": visits}
	visit_list, count_list = [], []
	data = {col_name: visit_list, "Count": count_list}

	for key, value in tables.get(col_name).items():
		visit_list.append(key)
		count_list.append(value)

	df = pd.DataFrame(data)
	return df


@app.page('/visits')
async def domain_statistic_view(web, request):
	return web.html(create_df("Visits").to_html(col_space=50, index=False))


if __name__ == '__main__':
	app.main()

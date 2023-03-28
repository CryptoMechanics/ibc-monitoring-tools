# core imports
import os
import logging
import json, typing
from datetime import datetime, timedelta
from decimal import Decimal
from dateutil import parser
import asyncio

# external library imports
import simplejson as json
import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, WebSocket
from fastapi.responses import Response, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import asyncpg

# project imports
from src.utils import load_dataframe, load_json

POSTGRES_USER = os.getenv('POSTGRES_USER', '')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', '')
POSTGRES_DB = os.getenv('POSTGRES_DB', '')
ACTION_COLLECTION_START_TIME = parser.parse(os.getenv('ACTION_COLLECTION_START_TIME', '2023-01-01'))
MATCHING_START_TIME = parser.parse(os.getenv('MATCHING_START_TIME', '2023-01-01'))
LOGGING_LEVEL = os.getenv('LOGGING_LEVEL', 'INFO').upper()
SECRET_KEY = os.getenv('SECRET_KEY', None)

# logging configuration
logging.basicConfig(
	level=LOGGING_LEVEL,
	format='%(asctime)s - %(levelname)s - %(message)s',
	datefmt='%Y-%m-%d %H:%M:%S'
)

pd.options.display.float_format = '{:.8f}'.format

class PrettyJSONResponse(Response):
    media_type = "application/json"

    def render(self, content: typing.Any) -> bytes:
        return json.dumps(
            content,
            ensure_ascii=False,
            allow_nan=False,
            indent=4,
            separators=(", ", ": "),
        ).encode("utf-8")

tags_metadata = [
    {
        "name": "General Data",
        "description": ""
    }
]

app = FastAPI(
    title="Antelope IBC Token Monitor API",
    version="0.1",
    openapi_tags=tags_metadata
)

# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["http://localhost", "http://localhost:8080", "http://localhost:8787"],
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )


@app.get('/trigger-unmatched-proof-alert', tags=['General Data'])
async def trigger_unmatched_proof_alert(fmt: str = 'json', secret_key: str = ''):
	"""
	An endpoint for adding a spurious unmatched proof to the database as a trigger to check the accounting alerts work as expected.
	"""

	await asyncio.sleep(1)

	if not SECRET_KEY:
		raise HTTPException(status_code=403, detail='No SECRET_KEY is configured in config.env')

	if secret_key != SECRET_KEY:
		raise HTTPException(status_code=403, detail='secret_key does not match SECRET_KEY in config.env')

	# get table name for first chain in chains.json
	with open('chains.json', 'r') as chains_file:
		chains = json.load(chains_file)
		chain, _ = list(chains.items())[0]
		table_name = f'actions_{chain}'

	conn = await asyncpg.connect(user=POSTGRES_USER, password=POSTGRES_PASSWORD, database=POSTGRES_DB, host='database')
	try:
		await conn.execute(f"""INSERT INTO {table_name} (
				global_sequence_id, tx, time, source_chain, destination_chain, action, owner, beneficiary, contract, symbol, quantity, xfer_global_sequence_id
				) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)""",
				12345678987654321, 'test_unmatched_proof_tx', datetime.utcnow(), chain, chain, 'issuea', 'owner', 'beneficiary', 'contract', 'symbol', 0.1234, 12345678987654321
			)
	finally:
		await conn.close()

	return {'detail': f'Added unmatched proof to {chain}_actions table'}


@app.get('/', tags=['Default Page'])
async def index():
	"""
	A simple page to summarise the status of monitoring and provide links to other pages.
	"""

	try:
		indexer_health_statuses = load_json('indexer_health_status')
		indexer_problem = 'DOWN' in [v['status'] for k,v in indexer_health_statuses.items()]

		df_successful_transfers = load_dataframe('successful_transfers')
		df_outstanding_transfers = load_dataframe('outstanding_transfers')
		df_unmatched_proofs = load_dataframe('unmatched_proofs')
		df_successful_transfers_with_discrepancies = load_dataframe('successful_transfers_with_discrepancies')

		# exclude proofs if source indexers are down
		df_unmatched_proofs = df_unmatched_proofs[df_unmatched_proofs['source_chain'].apply(lambda x: indexer_health_statuses[x]['status']) == 'UP']

	except FileNotFoundError as e:
		return HTTPException(status_code=404, detail="Please wait for data indexing to finish.")

	# make summary dataframes
	df_successful_transfers_summary = df_successful_transfers.groupby(by=['source_chain', 'destination_chain', 'source_action', 'symbol']).agg({'quantity': 'sum', 'symbol': 'size'})
	df_successful_transfers_summary = df_successful_transfers_summary.rename(columns={'symbol': 'count'})

	df_outstanding_transfers_summary = df_outstanding_transfers.groupby(by=['source_chain', 'destination_chain', 'source_action', 'symbol']).agg({'quantity': 'sum', 'symbol': 'size'})
	df_outstanding_transfers_summary = df_outstanding_transfers_summary.rename(columns={'symbol': 'count'})

	new_index = df_successful_transfers_summary.index.union(df_outstanding_transfers_summary.index)
	df_successful_transfers_summary = df_successful_transfers_summary.reindex(index=new_index).fillna(0)
	df_outstanding_transfers_summary = df_outstanding_transfers_summary.reindex(index=new_index).fillna(0)

	df_successful_transfers_ratio_summary = df_successful_transfers_summary / (df_successful_transfers_summary + df_outstanding_transfers_summary)

	html1 = ''
	if indexer_problem:
		html1 += '<h2>One or more action indexers has a problem which may invalidate the data below</h2>'
		html1 += f'<code>{indexer_health_statuses}</code>'

	if len(df_unmatched_proofs.index) > 0:
		html1 += '<h2><span style="color:red;font-size:20px">Unmatched Proofs</span></h2>'
		html1 += '<a href="/discrepancies?fmt=html">More Details</a>'
	else:
		html1 += '<h2>No Unmatched Proofs Detected</h2>'
	if len(df_successful_transfers_with_discrepancies.index) > 0:
		html1 += '<h2><span style="color:red;font-size:20px">Transfers with Proof Discrepancies</span></h2>'
		html1 += '<a href="/discrepancies?fmt=html">More Details</a>'
	else:
		html1 += '<h2>No Proof Discrepancies Detected</h2>'

	html2 = ''
	if len(df_successful_transfers_summary.index) > 0:
		df_successful_transfers_summary['quantity'] = df_successful_transfers_summary['quantity'].apply(lambda x: '{:,.4f}'.format(x))
		df_successful_transfers_summary['count'] = df_successful_transfers_summary['count'].apply(lambda x: str(int(x)))
		html2 += '<h2>Successful Transfers Summary</h2>'
		html2 += df_successful_transfers_summary.to_html(index=True, float_format='{:.4f}'.format) + '<br>'
	else:
		html2 += '<h2>No Successful Transfers</h2>'

	html3 = ''
	if len(df_outstanding_transfers_summary.index) > 0:
		df_outstanding_transfers_summary['quantity'] = df_outstanding_transfers_summary['quantity'].apply(lambda x: '{:,.4f}'.format(x))
		df_outstanding_transfers_summary['count'] = df_outstanding_transfers_summary['count'].apply(lambda x: str(int(x)))
		html3 += '<h2>Outstanding Transfers Summary</h2>'
		html3 += df_outstanding_transfers_summary.to_html(index=True) + '<br>'
	else:
		html3 += '<h2>No Outstanding Transfers</h2>'

	html4 = ''
	if len(df_successful_transfers_ratio_summary.index) > 0:
		html4 += '<h2>Transfer Success Ratios</h2>'
		html4 += df_successful_transfers_ratio_summary.to_html(index=True, float_format='{:.1%}'.format) + '<br>'

	outer_html = """
	<html>
		<head>
			<title>Antelope IBC Token Monitor</title>
			<meta name="description" content="">
			<style>
				body {
					font-family: Arial, sans-serif;
					background-color: #f1f1f1;
				}
				h1 {
					padding-top: 10px;
					color: #333;
					text-align: center;
				}
				.mydata {
					text-align: center;
					font-size: 16px;
					line-height: 1.5;
					color: #333;
					padding: 10px;
					background-color: #fff;
					box-shadow: 0 2px 5px rgba(0,0,0,0.1);
					border-radius: 5px;
					margin: 20px auto;
					max-width: 800px
				}
				.mydata h2 {
					font-size: 16px;
				}
				.dataframe {
					margin: 0 auto;
					halign: center;
				}
				.mydata .dataframe {
					text-align: right;
				}
				table {
					color: #333;
					border-collapse: collapse;
				}
				td {
					border: 1px solid black;
					padding: 2px;
				}
				th {
					border: 1px solid black;
					padding: 4px;
				}
				.note {
					text-align: center;
					padding: 3px;
					font-size: 14px;
				}
			</style>
		</head>
		<body>
			<h1>Antelope IBC Token Monitor</h1>
			<div class="mydata">
				""" + html1 + """
			</div>
			<div class="mydata">
				""" + html2 + """
			</div>
			<div class="mydata">
				""" + html3 + """
			</div>
			<div class="mydata">
				""" + html4 + """
			</div>
			<div class="note">ACTION_COLLECTION_START_TIME: """ + str(ACTION_COLLECTION_START_TIME) + """</div>
			<div class="note">MATCHING_START_TIME: """ + str(MATCHING_START_TIME) + """</div>
			<div class="note" style="margin-top:8px">
				<a href="/docs" target="_blank">Full OpenAPI Documentation</a>
			</div>
			<br>
		</body>
	</html>
	"""

	return HTMLResponse(outer_html)


@app.get('/indexer-health-status', tags=["General Data"], response_class=PrettyJSONResponse)
async def indexer_health_status(fmt: str = 'json'):
	"""
	Provides info about whether each action indexer is UP or DOWN, and when it was last checked.

	Args:
	- fmt: Optional string indicating the format of the response, either 'json' or 'html'.

	Returns:
		A JSON or HTML response containing data related to the health of each action indexer, depending on the value of *fmt*.
	"""

	try:
		indexer_health_statuses = load_json('indexer_health_status')
		indexer_problem = 'DOWN' in [v['status'] for k,v in indexer_health_statuses.items()]
	except FileNotFoundError as e:
		return HTTPException(status_code=404, detail="This data could not be found. It may not yet have been generated.")

	if fmt == 'json':
		return indexer_health_statuses
	else:
		html = ''
		if indexer_problem:
			html += '<h2>One or more indexers has a problem:</h2>'
			html += f'<code>{indexer_health_statuses}</code>'
		else:
			html += '<h2>All actions indexers are healthy.</h2>'
			html += f'<code>{indexer_health_statuses}</code>'
		return HTMLResponse(html)

@app.get('/discrepancies', tags=["General Data"], response_class=PrettyJSONResponse)
async def discrepancies(start: datetime = None, end: datetime = None, fmt: str = 'json'):
	"""
	Provides info about important source/destination action discrepancies.

	Args:
	- fmt: Optional string indicating the format of the response, either 'json' or 'html'.

	Returns: A JSON or HTML response containing the data, depending on the value of *fmt*.
	- A object containing the status of each action indexer - whether they are Up or DOWN, and the time last queried.
	- A list of records corresponding to proofs that have succeeded on the destination chain(s), but don't have a corresponding locking action on the source chain(s).
	- A list of records corresponding to transfers whose locking action on the source chain didn't match the corresponding unlocking action on the destination chain.
	"""

	indexer_health_statuses = load_json('indexer_health_status')
	df_unmatched_proofs = load_dataframe('unmatched_proofs')
	df_matched_with_discrepancies = load_dataframe('successful_transfers_with_discrepancies')

	if start:
		df_unmatched_proofs = df_unmatched_proofs[df_unmatched_proofs['time'] >= start]
		df_matched_with_discrepancies = df_matched_with_discrepancies[df_matched_with_discrepancies['time'] >= start]

	if end:
		df_unmatched_proofs = df_unmatched_proofs[df_unmatched_proofs['time'] < end]
		df_matched_with_discrepancies = df_matched_with_discrepancies[df_matched_with_discrepancies['time'] < end]

	# exclude proofs if source indexers are down
	df_unmatched_proofs = df_unmatched_proofs[df_unmatched_proofs['source_chain'].apply(lambda x: indexer_health_statuses[x]['status']) == 'UP']

	if fmt == 'json':
		data = {
			'indexer_health_statuses': indexer_health_statuses,
			'transfers_with_discrepancies': df_matched_with_discrepancies.to_dict(orient='records'),
			'unmatched_proofs': df_unmatched_proofs.to_dict(orient='records')
		}
		return data
	else:
		html = ''
		if len(df_unmatched_proofs.index) > 0:
			html += '<h2>Unmatched Proofs</h2>'
			html += df_unmatched_proofs.to_html(index=True) + '<br>'
		else:
			html += '<h2>No Unmatched Proofs Detected</h2>'
		if len(df_matched_with_discrepancies.index) > 0:
			html += '<h2>Transfers with Discrepancies</h2>'
			html += df_matched_with_discrepancies.to_html(index=True) + '<br>'
		else:
			html += '<h2>No Transfers with Discrepancies Detected</h2>'
		return HTMLResponse(html)


@app.get('/transfers', tags=["General Data"], response_class=PrettyJSONResponse)
async def transfers(start: datetime = None, end: datetime = None, fmt: str = 'json'):
	"""
	Provides raw data related to IBC token transfers, both complete and partial.

	There are different sections for:
	- indexer health status
	- transfers that have been completed successfully
	- transfers which have been initiated, but not proven on the destination chain

	Args:
	- start: Optional datetime object representing the start of the date range to filter by.
	- end: Optional datetime object representing the end of the date range to filter by.
	- fmt: Optional string indicating the format of the response, either 'json' or 'html'.

	Returns:
		A JSON or HTML response containing data related to transfers, depending on the value of *fmt*.
	"""

	try:
		indexer_health_statuses = load_json('indexer_health_status')
		indexer_problem = 'DOWN' in [v['status'] for k,v in indexer_health_statuses.items()]

		df_successful_transfers = load_dataframe('successful_transfers')
		df_outstanding_transfers = load_dataframe('outstanding_transfers')
	except FileNotFoundError as e:
		return HTTPException(status_code=404, detail="This data could not be found. It may not yet have been generated.")

	if start:
		df_successful_transfers = df_successful_transfers[df_successful_transfers['source_time'] >= start]
		df_outstanding_transfers = df_outstanding_transfers[df_outstanding_transfers['time'] >= start]

	if end:
		df_successful_transfers = df_successful_transfers[df_successful_transfers['source_time'] < end]
		df_outstanding_transfers = df_outstanding_transfers[df_outstanding_transfers['time'] < end]

	if fmt == 'json':
		data = {
			'indexer_health_statuses': indexer_health_statuses,
			'successful_transfers': df_successful_transfers.to_dict(orient='records'),
			'outstanding_transfers': df_outstanding_transfers.to_dict(orient='records'),
		}
		return data
	else:
		html = ''
		if indexer_problem:
			html += '<h2>One or more action indexers has a problem which may invalidate the data below</h2>'
			html += f'<code>{indexer_health_statuses}</code>'
		html += '<h2>Successful Transfers</h2>'
		html += df_successful_transfers.to_html(index=True) + '<br>'
		html += '<h2>Outstanding Transfers</h2>'
		html += df_outstanding_transfers.to_html(index=True) + '<br>'
		return HTMLResponse(html)

@app.get('/transfers-summary', tags=["General Data"], response_class=PrettyJSONResponse)
async def transfers_summary(start: datetime = None, end: datetime = None, fmt: str = 'json'):
	"""
	Provides summary data related to IBC token transfers, both complete and partial.

	There are different sections for:
	- indexer health status
	- transfers that have been completed successfully
	- transfers which have been initiated, but not proven on the destination chain

	Args:
	- start: Optional datetime object representing the start of the date range to filter by.
	- end: Optional datetime object representing the end of the date range to filter by.
	- fmt: Optional string indicating the format of the response, either 'json' or 'html'.

	Returns:
		A JSON or HTML response containing data related to transfers, depending on the value of *fmt*.
	"""

	try:
		indexer_health_statuses = load_json('indexer_health_status')
		indexer_problem = 'DOWN' in [v['status'] for k,v in indexer_health_statuses.items()]

		df_successful_transfers = load_dataframe('successful_transfers')
		df_outstanding_transfers = load_dataframe('outstanding_transfers')
	except FileNotFoundError as e:
		return HTTPException(status_code=404, detail="This data could not be found. It may not yet have been generated.")

	if start:
		df_successful_transfers = df_successful_transfers[df_successful_transfers['source_time'] >= start]
		df_outstanding_transfers = df_outstanding_transfers[df_outstanding_transfers['time'] >= start]

	if end:
		df_successful_transfers = df_successful_transfers[df_successful_transfers['source_time'] < end]
		df_outstanding_transfers = df_outstanding_transfers[df_outstanding_transfers['time'] < end]

	# make summary dataframes
	df_successful_transfers_summary = df_successful_transfers.groupby(by=['source_chain', 'destination_chain', 'source_action', 'symbol']).agg({'quantity': 'sum', 'symbol': 'size'})
	df_successful_transfers_summary = df_successful_transfers_summary.rename(columns={'symbol': 'count'})

	df_outstanding_transfers_summary = df_outstanding_transfers.groupby(by=['source_chain', 'destination_chain', 'source_action', 'symbol']).agg({'quantity': 'sum', 'symbol': 'size'})
	df_outstanding_transfers_summary = df_outstanding_transfers_summary.rename(columns={'symbol': 'count'})

	new_index = df_successful_transfers_summary.index.union(df_outstanding_transfers_summary.index)
	df_successful_transfers_summary = df_successful_transfers_summary.reindex(index=new_index).fillna(0)
	df_outstanding_transfers_summary = df_outstanding_transfers_summary.reindex(index=new_index).fillna(0)

	df_successful_transfers_ratio_summary = df_successful_transfers_summary / (df_successful_transfers_summary + df_outstanding_transfers_summary)

	if fmt == 'json':
		data = {
			'indexer_health_statuses': indexer_health_statuses,
			'successful_transfers_summary': df_successful_transfers_summary.reset_index().to_dict(orient='records'),
			'outstanding_transfers_summary': df_outstanding_transfers_summary.reset_index().to_dict(orient='records'),
			'df_successful_transfers_ratio_summary': df_successful_transfers_ratio_summary.reset_index().to_dict(orient='records')
		}
		return data
	else:
		html = ''
		if indexer_problem:
			html += '<h2>One or more action indexers has a problem which may invalidate the data below</h2>'
			html += f'<code>{indexer_health_statuses}</code>'
		if len(df_successful_transfers_summary.index) > 0:
			df_successful_transfers_summary['quantity'] = df_successful_transfers_summary['quantity'].apply(lambda x: '{:,.4f}'.format(x))
			df_successful_transfers_summary['count'] = df_successful_transfers_summary['count'].apply(lambda x: str(int(x)))
			html += '<h2>Successful Transfers Summary</h2>'
			html += df_successful_transfers_summary.to_html(index=True, float_format='{:.4f}'.format) + '<br>'
		else:
			html += '<h2>No Successful Transfers</h2>'
		if len(df_outstanding_transfers_summary.index) > 0:
			df_outstanding_transfers_summary['quantity'] = df_outstanding_transfers_summary['quantity'].apply(lambda x: '{:,.4f}'.format(x))
			df_outstanding_transfers_summary['count'] = df_outstanding_transfers_summary['count'].apply(lambda x: str(int(x)))
			html += '<h2>Outstanding Transfers Summary</h2>'
			html += df_outstanding_transfers_summary.to_html(index=True) + '<br>'
		else:
			html += '<h2>No Outstanding Transfers</h2>'
		if len(df_successful_transfers_ratio_summary.index) > 0:
			html += '<h2>Transfer Success Ratios</h2>'
			html += df_successful_transfers_ratio_summary.to_html(index=True, float_format='{:.1%}'.format) + '<br>'
		return HTMLResponse(html)

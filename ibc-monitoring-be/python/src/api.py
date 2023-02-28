# core imports
import os
import logging
import json, typing
from datetime import datetime, timedelta
from decimal import Decimal

# external library imports
import simplejson as json
import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, WebSocket
from fastapi.responses import Response, HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# project imports
from src.utils import load_dataframe, load_json

# logging configuration
logging.basicConfig(
	level=logging.DEBUG,
	format='%(asctime)s %(levelname)s %(message)s',
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
    title="IBC Monitor API",
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

@app.get('/', tags=["General Data"], response_class=PrettyJSONResponse)
async def index():
	"""
	TODO: Basic UI - scope TBD
	"""

	return 'TODO: Basic UI - scope TBD'


@app.get('/indexer-health-status', tags=["General Data"], response_class=PrettyJSONResponse)
async def indexer_health_status(fmt: str = 'json'):
	"""
	Retreives data about the health of the action indexers.

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


@app.get('/transfers', tags=["General Data"], response_class=PrettyJSONResponse)
async def transfers(start: datetime = None, end: datetime = None, fmt: str = 'json'):
	"""
	Retrieves raw data related to IBC token transfers.

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
		df_unmatched_proofs = load_dataframe('unmatched_proofs')
		df_successful_transfers_with_discrepancies = load_dataframe('successful_transfers_with_discrepancies')
	except FileNotFoundError as e:
		return HTTPException(status_code=404, detail="This data could not be found. It may not yet have been generated.")

	if start:
		df_successful_transfers = df_successful_transfers[df_successful_transfers['source_time'] >= start]
		df_successful_transfers_with_discrepancies = df_successful_transfers_with_discrepancies[df_successful_transfers_with_discrepancies['source_time'] >= start]
		df_outstanding_transfers = df_outstanding_transfers[df_outstanding_transfers['time'] >= start]
		df_unmatched_proofs = df_unmatched_proofs[df_unmatched_proofs['time'] >= start]

	if end:
		df_successful_transfers = df_successful_transfers[df_successful_transfers['source_time'] < end]
		df_successful_transfers_with_discrepancies = df_successful_transfers_with_discrepancies[df_successful_transfers_with_discrepancies['source_time'] < end]
		df_outstanding_transfers = df_outstanding_transfers[df_outstanding_transfers['time'] < end]
		df_unmatched_proofs = df_unmatched_proofs[df_unmatched_proofs['time'] < end]

	# df_successful_transfers = df_successful_transfers.applymap(lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ") if isinstance(x, pd.Timestamp) else x)
	# df_outstanding_transfers = df_outstanding_transfers.applymap(lambda x: x.strftime("%Y-%m-%dT%H:%M:%SZ") if isinstance(x, pd.Timestamp) else x)

	if fmt == 'json':
		data = {
			'indexer_health_statuses': indexer_health_statuses,
			'successful_transfers': df_successful_transfers.to_dict(orient='records'),
			'successful_transfers_with_discrepancies': df_successful_transfers_with_discrepancies.to_dict(orient='records'),
			'outstanding_transfers': df_outstanding_transfers.to_dict(orient='records'),
			'unmatched_proofs': df_unmatched_proofs.to_dict(orient='records')
		}
		return data
	else:
		html = ''
		if indexer_problem:
			html += '<h2>One or more action indexers has a problem which may invalidate the data below</h2>'
			html += f'<code>{indexer_health_statuses}</code>'
		if len(df_unmatched_proofs.index) > 0:
			html += '<h2>Unmatched Proofs</h2>'
			html += df_unmatched_proofs.to_html(index=True) + '<br>'
		else:
			html += '<h2>No Unmatched Proofs Detected</h2>'
		if len(df_successful_transfers_with_discrepancies.index) > 0:
			html += '<h2>Successful Transfers with Discrepancies</h2>'
			html += df_successful_transfers_with_discrepancies.to_html(index=True) + '<br>'
		else:
			html += '<h2>No Successful Transfers with Discrepancies Detected</h2>'
		html += '<h2>Successful Transfers</h2>'
		html += df_successful_transfers.to_html(index=True) + '<br>'
		html += '<h2>Outstanding Transfers</h2>'
		html += df_outstanding_transfers.to_html(index=True) + '<br>'
		return HTMLResponse(html)

@app.get('/transfers-summary', tags=["General Data"], response_class=PrettyJSONResponse)
async def transfers_summary(start: datetime = None, end: datetime = None, fmt: str = 'json'):
	"""
	Retrieves summary data related to IBC token transfers.

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
		df_unmatched_proofs = load_dataframe('unmatched_proofs')
		df_successful_transfers_with_discrepancies = load_dataframe('successful_transfers_with_discrepancies')
	except FileNotFoundError as e:
		return HTTPException(status_code=404, detail="This data could not be found. It may not yet have been generated.")

	if start:
		df_successful_transfers = df_successful_transfers[df_successful_transfers['source_time'] >= start]
		df_successful_transfers_with_discrepancies = df_successful_transfers_with_discrepancies[df_successful_transfers_with_discrepancies['source_time'] >= start]
		df_outstanding_transfers = df_outstanding_transfers[df_outstanding_transfers['time'] >= start]
		df_unmatched_proofs = df_unmatched_proofs[df_unmatched_proofs['time'] >= start]

	if end:
		df_successful_transfers = df_successful_transfers[df_successful_transfers['source_time'] < end]
		df_successful_transfers_with_discrepancies = df_successful_transfers_with_discrepancies[df_successful_transfers_with_discrepancies['source_time'] < end]
		df_outstanding_transfers = df_outstanding_transfers[df_outstanding_transfers['time'] < end]
		df_unmatched_proofs = df_unmatched_proofs[df_unmatched_proofs['time'] < end]

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
			'unmatched_proofs': df_unmatched_proofs.to_dict(orient='records'),
			'successful_transfers_with_discrepancies': df_successful_transfers_with_discrepancies.to_dict(orient='records'),
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
		if len(df_unmatched_proofs.index) > 0:
			html += '<h2>Unmatched Proofs</h2>'
			html += df_unmatched_proofs.to_html(index=True) + '<br>'
		else:
			html += '<h2>No Unmatched Proofs Detected</h2>'
		if len(df_successful_transfers_with_discrepancies.index) > 0:
			html += '<h2>Successful Transfers with Discrepancies</h2>'
			html += df_successful_transfers_with_discrepancies.to_html(index=True) + '<br>'
		else:
			html += '<h2>No Successful Transfers with Discrepancies Detected</h2>'
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

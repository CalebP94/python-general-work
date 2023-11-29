from flask import Flask, request, jsonify
from flask_cors import CORS
from process_ffcf import ProcessingENV
import asyncio
# import requests
# import asyncio
# import aiohttp

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})
env = ProcessingENV()


@app.route("/")
async def cash_flow_symbol():
    return "cash flow shit"

# @app.route('/cashFlow')
# def process_cash_flow():
#    cashFlow_return = env.process_free_cash_flow()
#    return cashFlow_return

@app.route("/cashFlow", methods=["GET"])
async def process_future_cashFlow():
    data = await env.get_requests_cash_flow_statement()
    balanceSheetData= await env.get_requests_balance_sheet()
    cash_equivalent = float(balanceSheetData[0]["cashAndCashEquivalentsAtCarryingValue"])
    balanceSheet_debt = float(balanceSheetData[0]["currentDebt"])
    commonStock_outstanding = float(balanceSheetData[0]["commonStockSharesOutstanding"])

    ffcf = env.run(data, balanceSheetData_ce=cash_equivalent, balanceSheetData_debt=balanceSheet_debt, commonStock=commonStock_outstanding)
    
    return_dict = dict({})

    return_dict["balance-sheet"] = balanceSheetData
    return_dict["cash-flow-statement"] = data
    return_dict["cash_equivalent"] = cash_equivalent
    return_dict["balance-sheet-debt"] = balanceSheet_debt
    return_dict["common-stock-outstanding"] = commonStock_outstanding
    return_dict["ffcf-final"] = ffcf


    return return_dict

@app.route("/balanceSheet", methods=["GET"])
async def process_balance_sheet():
    data = await env.get_requests_balance_sheet()
    return data

@app.route("/rawCF", methods=["GET"])
async def raw_cfs():
    data = env.get_requests_cash_flow_statement()
    return data

@app.route('/cashFlow/updateSticker', methods=['POST'])
async def receive_sticker():
    data = await request.json  # Assuming the data is sent in JSON format
    # Process the data as needed
    return data
    # result = {'message': 'Data received successfully'}
    # return jsonify(result)

if __name__ == '__main__':

    app.run()





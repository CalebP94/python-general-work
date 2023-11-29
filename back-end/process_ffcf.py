# import requests
import datetime
# import asyncio
import aiohttp
# import httpx

class ProcessingENV():
    def __init__(self) -> None:
        #testing server
        serv = 'http://127.0.0.1:5000/'
        #api calls
        self.personal_key = 'GUWCRACFIF1S1ID7'
        self.stock_sticker = 'IBM'
        self.intra_day_timeseries = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={}&interval=5min&apikey={}'.format(self.stock_sticker,self.personal_key)
        self.cash_flow = 'https://www.alphavantage.co/query?function=CASH_FLOW&symbol={}&apikey={}'.format(self.stock_sticker,self.personal_key)
        self.balance_sheet = 'https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={}&apikey={}'.format(self.stock_sticker,self.personal_key)
        self.cashFlow_key = "CFA_{}".format(self.stock_sticker)
        #future free cash flow details
        self.growth_total = 0
        self.stock_history_count = 0
        self.future_freeCash_forecast = dict({})
        # #terminal value 
        self.discount_rate = .08
        self.perpetual_growt = .025
        # self.terminal_cashFlow = self.__terminalValue()
        #under the hood
        # self.cashFlow_analysis_arr = self.__process_free_cash_flow()

    async def get_requests_balance_sheet(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.balance_sheet) as response:
                if response.status == 200:
                    data = await response.json()
                    return data['annualReports']
                else:
                    return response.status

    async def get_requests_time_series(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.intra_day_timeseries) as response:
                if response.status == 200:
                    data = await response.json()
                    return data['annualReports']
                else:
                    return response.status
        
    async def get_requests_cash_flow_statement(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(self.cash_flow) as response:
                if response.status == 200:
                    data = await response.json()
                    return data['annualReports']
                else:
                    return response.status
                
    def setStickerKey(self, sticker=None):
        if(sticker == None):
            self.intra_day_timeseries = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={}&interval=5min&apikey={}'.format(self.stock_sticker,self.personal_key)
            self.cash_flow = 'https://www.alphavantage.co/query?function=CASH_FLOW&symbol={}&apikey={}'.format(self.stock_sticker,self.personal_key)
            self.balance_sheet = 'https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={}&apikey={}'.format(self.stock_sticker,self.personal_key)
        else:
            self.intra_day_timeseries = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={}&interval=5min&apikey={}'.format(sticker,self.personal_key)
            self.cash_flow = 'https://www.alphavantage.co/query?function=CASH_FLOW&symbol={}&apikey={}'.format(sticker,self.personal_key)
            # self.balance_sheet = 'https://www.alphavantage.co/query?function=BALANCE_SHEET&symbol={}&apikey={}'.format(sticker 6,self.personal_key)
    
    def __process_free_cash_flow(self, ffcf_data):
        freeCashFlow_dict = dict({})
        cashFlow_analysis_arr = []
        statement_arr = ffcf_data
        counter = len(statement_arr) - 1
        self.stock_history_count = len(statement_arr)
        while counter > -1:
            currentObj = statement_arr[counter]
            free_cashFlow = float(currentObj["operatingCashflow"]) - float(currentObj["capitalExpenditures"])
            key = currentObj["fiscalDateEnding"]
            freeCashFlow_dict[key] = {"freeCashFlow":free_cashFlow}
            if counter < 4:
                grabber = counter + 1
                grabbed = statement_arr[grabber]
                free_cashFlowPrevious = float(grabbed["operatingCashflow"]) - float(grabbed["capitalExpenditures"])
                percentGain = (free_cashFlow - free_cashFlowPrevious)/free_cashFlowPrevious
                freeCashFlow_dict[key] = {"freeCashFlow":free_cashFlow, "percentGain":percentGain}
            counter = counter - 1
        keys = list(freeCashFlow_dict.keys())
        if self.growth_total == 0:
            for row in freeCashFlow_dict:
                keys = list(freeCashFlow_dict[row].keys())
                if len(keys) > 1:
                    self.growth_total += freeCashFlow_dict[row]['percentGain']
                continue
        
        averageGrowth = self.growth_total/self.stock_history_count
        averageGrowth_dict=dict({"averageGrowthRate":averageGrowth})
        cashFlow_analysis_arr.append(averageGrowth_dict)
        cashFlow_analysis_arr.append(freeCashFlow_dict)

        return cashFlow_analysis_arr

    def __compute_future_freeCash(self, cashFlow_analysis_arr):
        future_freeCash_forecast = dict({})
        #how many years to project
        desiredYear = 2030
        currentYear = datetime.date.today().year 
        numberOfYears = desiredYear - currentYear
        #average growth rate
        averageGrowth = cashFlow_analysis_arr[0]["averageGrowthRate"]
        dateStringsArr = list(cashFlow_analysis_arr[1].keys())
        lastYearDatTime = datetime.datetime.strptime(dateStringsArr[-1], "%Y-%m-%d").date()
        #access the last year on record's free cash flow
        searchYear = dateStringsArr[-1]
        searchObj = cashFlow_analysis_arr[1]
        searchYearArr = []
        #structure future free cash flow estimate object
        for i in range(numberOfYears+1):
            j = i + 1
            a = i - 1
            nextYearDateTime = str(lastYearDatTime.replace(year=lastYearDatTime.year + j))
            searchYearArr.append(nextYearDateTime)
            if j == 1:
                futureCash = ((searchObj[searchYear]["freeCashFlow"]) * (1+averageGrowth))
                future_freeCash_forecast[nextYearDateTime] = {"futureFreeCashFlowEst":futureCash}
            elif j > 1 and j <= (numberOfYears + 1):
                futureCash = ((future_freeCash_forecast[searchYearArr[a]]["futureFreeCashFlowEst"]) * (1+averageGrowth))
                future_freeCash_forecast[nextYearDateTime] = {"futureFreeCashFlowEst":futureCash}
            else:
                return "Something went wrong"

        return [future_freeCash_forecast, {"averageGrowthRate":averageGrowth}]
    
    def __terminalValue(self, ffcf):
        future_free_cash_flow = ffcf[0]
        if len(list(ffcf[0].keys())) > 0:
            lastYear = list(future_free_cash_flow.keys())[-1]
            lastYearValue = future_free_cash_flow[lastYear]["futureFreeCashFlowEst"]
            terminalValue = lastYearValue * ((1+self.perpetual_growt)/(self.discount_rate - self.perpetual_growt))
            
            pv_terminalValue_exponent = len(list(ffcf[0].keys()))
            print(pv_terminalValue_exponent)
            pv_temrinalValue = terminalValue/((1+self.discount_rate)** pv_terminalValue_exponent) 
            term_obj = {'terminalValue':terminalValue, 'pv_terminalValue':pv_temrinalValue}
            return term_obj
        else: 
            return "Something is wrong"
        
    def __pv_of_ffcf_and_ffcf(self, cashFlowArr):
        computedFFCF_FFC = cashFlowArr[0]
        computedFFCF_keys = cashFlowArr[0].keys()
        for i, row in enumerate(computedFFCF_keys):
            i = i + 1 
            pvfcc = float(computedFFCF_FFC[row]['futureFreeCashFlowEst'])/((1+self.discount_rate)**i)
            computedFFCF_FFC[row] ={"pvfcc":pvfcc, "futureFreeCashFlowEst":computedFFCF_FFC[row]["futureFreeCashFlowEst"]} 
        return computedFFCF_FFC
    
    # #def __structure_pv_ffcf(self):

    
    def __sum_of_ffcf(self, pv_ffcf):
        final_sum = 0
        sum_ffcf_arr = []
        dict_ffcf_pvffcf={}
        computedFFCF_PVFFCF_list = list(pv_ffcf.keys())
        computedFFCF_PVFFCF = pv_ffcf
        for i, row in enumerate(computedFFCF_PVFFCF):
            dict_ffcf_pvffcf.update({computedFFCF_PVFFCF_list[i]: computedFFCF_PVFFCF[row]})

        pv_ffcf_arr = list(dict_ffcf_pvffcf.keys())
 
        for row in pv_ffcf_arr:
            sum_ffcf_arr.append(dict_ffcf_pvffcf[row]['pvfcc'])

        for row in sum_ffcf_arr:
            final_sum += row

        return final_sum


    def stock_value(self, sumFfcf_value, balanceSheetData_ce, balanceSheetData_debt, commonStock):

        sum_ffcf = sumFfcf_value
        equity_value = ((float(sum_ffcf) + float(balanceSheetData_ce)) - float(balanceSheetData_debt))/float(commonStock)

        return equity_value
   
    
    def run(self, data,balanceSheetData_ce, balanceSheetData_debt, commonStock):
        processedFreeCashFlow = self.__process_free_cash_flow(data)
        #processedFreeCashFlow_keys = list(processedFreeCashFlow[1].keys())
        future_free_cash_flow = self.__compute_future_freeCash(processedFreeCashFlow)
        pvFfcf = self.__pv_of_ffcf_and_ffcf(future_free_cash_flow)
        terminal_value = self.__terminalValue(ffcf=future_free_cash_flow)
        sum_ffcf = self.__sum_of_ffcf(pvFfcf)
        stockValue = self.stock_value(sum_ffcf, balanceSheetData_ce=balanceSheetData_ce, balanceSheetData_debt=balanceSheetData_debt,commonStock=commonStock)

    
        cfa_dict = dict({"CFA":{"historic_fcf_avg_growth":processedFreeCashFlow, "projected_ffcf":future_free_cash_flow, "terminalValue":terminal_value, "equity_value":stockValue}})
        return cfa_dict
        # print(cfa_dict)
        # return cfa_dict




        
            



    
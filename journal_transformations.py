import pandas as pd
import os


class IvvcDataTrans ():
    def __init__(self) -> None:
        self.stuff = 'printer'
        self.python_script_dir= os.getcwd()
        self.journal_src = os.path.join(self.python_script_dir, "journal-sources")
        self.journal_output = os.path.join(self.journal_src, 'output')
        self.exports_raw = os.path.join(self.journal_src, 'exports_raw')
        self.cap_cred_file_path = ''
        self.cap_deb_file_path = ''
        self.om_cred_file_path = ''
        self.om_deb_file_path = ''

    def __stringConditionals(self, file_arr=[], journal_cat="", cat_version=""):
        if len(file_arr):
            try:
                if journal_cat == 'Capital' and cat_version == 'Credit':
                    if len(file_arr[0]) > 1:
                        self.cap_cred_file_path = "\\".join([self.exports_raw, file_arr[0][-1]])
                    elif len(file_arr[0]) == 1:
                        self.cap_cred_file_path = "\\".join([self.exports_raw, file_arr[0][0]])
                elif journal_cat == 'Capital' and cat_version == 'Debit':
                    if len(file_arr[1]) > 1:
                        self.cap_deb_file_path = "\\".join([self.exports_raw, file_arr[1][-1]])
                    elif len(file_arr[1]) == 1:
                        self.cap_deb_file_path = "\\".join([self.exports_raw, file_arr[1][0]])
                    else:
                        print("No capital capital debits")    
                elif journal_cat == 'OM' and cat_version == 'Credit':
                    if len(file_arr[1]) > 1:
                        self.om_cred_file_path = "\\".join([self.exports_raw, file_arr[2][-1]])
                    elif len(file_arr[1]) == 1:
                        self.om_cred_file_path = "\\".join([self.exports_raw, file_arr[2][0]])
                    else:
                        print("No capital capital debits")  
                elif journal_cat == 'OM' and cat_version == 'Debit':
                    if len(file_arr[1]) > 1:
                        self.om_deb_file_path = "\\".join([self.exports_raw, file_arr[3][-1]])
                    elif len(file_arr[1]) == 1:
                        self.om_deb_file_path = "\\".join([self.exports_raw, file_arr[3][0]])
                    else:
                        print("No capital capital debits")               
            except Exception as e:
                print(e)
        else:
            print("No files")

    def __buildStrings(self):
        print(self.exports_raw)
        directory_listed = [f for f in os.listdir(self.exports_raw)]
        print(directory_listed)
        capital_credits_file_names = [f for f in os.listdir(self.exports_raw) if f.split(' - ')[1] == 'Capital' and 'Credits'  in f]
        capital_credits_file_names.sort()
        print(capital_credits_file_names)
        capital_debits_file_names = [f for f in os.listdir(self.exports_raw) if f.split(' - ')[1] == 'Capital' and 'Debits' in f]
        capital_debits_file_names.sort()
        print(capital_debits_file_names)
        OM_debits_file_names = [f for f in os.listdir(self.exports_raw) if f.split(' - ')[1] == 'OM' and 'Debits' in f]
        OM_debits_file_names.sort()
        OM_credits_file_names = [f for f in os.listdir(self.exports_raw) if f.split(' - ')[1] == 'OM' and 'Credits' in f]
        OM_credits_file_names.sort()
        final_file_array = [capital_credits_file_names, capital_debits_file_names, OM_credits_file_names, OM_debits_file_names]
        print(final_file_array)


        self.__stringConditionals(final_file_array, journal_cat="Capital", cat_version="Credit")
        self.__stringConditionals(final_file_array, journal_cat="Capital", cat_version="Debit")
        self.__stringConditionals(final_file_array, journal_cat="OM", cat_version="Credit")
        self.__stringConditionals(final_file_array, journal_cat="OM", cat_version="Debit")
    
    def __transformation(self):
        cap_cred_data_frame = pd.read_csv(self.cap_cred_file_path)
        cap_deb_data_frame = pd.read_csv(self.cap_deb_file_path)
        om_cred_df = pd.read_csv(self.om_cred_file_path)
        om_deb_df = pd.read_csv(self.om_deb_file_path)
        
        cap_debit_melted_df = pd.melt(cap_deb_data_frame, id_vars = ['Journal Date', 'Journal Description', 'Account', 'Operating Unit', 'Responsibility Center', 'Project ID', 'Activity', 'Process', 'Line Description'], var_name=["Resource ID"])
        cap_credit_melted_df = pd.melt(cap_cred_data_frame, id_vars = ['Journal Date', 'Journal Description', 'Account', 'Operating Unit', 'Responsibility Center', 'Project ID', 'Activity', 'Process', 'Line Description'], var_name=["Resource ID"])
        om_credit_melted_df = pd.melt(om_cred_df, id_vars = ['Journal Date', 'Journal Description', 'Account', 'Operating Unit', 'Responsibility Center', 'Project ID', 'Activity', 'Process', 'Line Description'], var_name=["Resource ID"])
        om_deb_melted_df = pd.melt(om_deb_df, id_vars = ['Journal Date', 'Journal Description', 'Account', 'Operating Unit', 'Responsibility Center', 'Project ID', 'Activity', 'Process', 'Line Description'], var_name=["Resource ID"])

        cap_credit_melted_df["Note"] = cap_credit_melted_df["Resource ID"].apply(lambda x: 'Capital Journal Entry Credits' if x in ('69000', '11000') else 'Capital Journal Entry Credits Expenses')
        cap_debit_melted_df["Note"] = cap_debit_melted_df["Resource ID"].apply(lambda x: 'Capital Journal Entry Debits' if x in ('69000', '11000') else 'Capital Journal Entry Debits Expenses')
        om_credit_melted_df["Note"] = om_credit_melted_df["Resource ID"].apply(lambda x: 'OM Journal Entry Credits' if x in ('69000', '11000') else 'OM Journal Entry Credits Expenses')
        om_deb_melted_df["Note"] = om_deb_melted_df["Resource ID"].apply(lambda x: 'OM Journal Entry Debits' if x in ('69000', '11000') else 'OM Journal Entry Debits Expenses')

        capital_df = pd.concat([cap_debit_melted_df, cap_credit_melted_df, om_credit_melted_df, om_deb_melted_df], ignore_index=True)
        capital_df.index = range(1, len(capital_df) + 1)

        capital_df.to_excel(os.path.join(self.journal_output, "20231128 Capital Export IVVC2 Journal.xlsx"))

    def run(self):
        self.__buildStrings()
        self.__transformation()

    
if __name__ == "__main__":
    data_trans = IvvcDataTrans()
    data_trans.run()
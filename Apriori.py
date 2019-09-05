from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
import pandas as pd
import pickle
import os
pd.set_option('display.max_columns', 999)
pd.set_option('display.width', 999)
totalDf = pd.DataFrame()

anual = True
if anual:
    for loja in os.listdir('lojas_anual'):
        print(loja)
        tradutor = pickle.load(open('tradutores/tradutor_loja_' + loja,'rb'))
        path = 'lojas_anual/' + loja
        dataset = pickle.load(open(path, 'rb'))
        te = TransactionEncoder()
        transaction_len = len(dataset)
        # print(dataset)
        te_ary = te.fit(dataset)
        te_ary = te_ary.transform(dataset)
        df = pd.SparseDataFrame(te_ary, columns=te.columns_)
        frequent_item_set = apriori(df, min_support = 0.01, use_colnames = True, max_len = 2)
        # print(frequent_item_set)
        # df = pd.SparseDataFrame(frequent_item_set)
        df = association_rules(df, metric="confidence", min_threshold = 0.15)
        df['Nconfidence'] = df['support'] / df['consequent support']
        df['loja'] = loja
        df['antecedents'] = df['antecedents'].apply(lambda a: tradutor[int(str(a).split('{')[1].split('}')[0])])
        df['consequents'] = df['consequents'].apply(lambda a: tradutor[int(str(a).split('{')[1].split('}')[0])])
        # df['antecedents'] = df['antecedents'].apply(lambda a: tradutor[a])
        # df['consequents'] = df['consequents'].apply(lambda a: tradutor[a])
        if len(totalDf) == 0:
            totalDf = df
        else:
            totalDf = totalDf.append(df)
        # print(totalDf)
        pass
else:
    for loja in os.listdir('lojas'):
        print(loja)
        tradutor = pickle.load(open('tradutores/tradutor_loja_' +loja + '.pickle','rb'))
        for dia in os.listdir('lojas/' + loja):
            print(dia.split('.')[0])
            path = 'lojas/' + loja + '/' + dia
            dataset = pickle.load(open(path, 'rb'))
            te = TransactionEncoder()
            transaction_len = len(dataset)
            # print(dataset)
            te_ary = te.fit(dataset).transform(dataset)
            df = pd.DataFrame(te_ary, columns=te.columns_)
            frequent_item_set = apriori(df, min_support = 0.01, use_colnames = True, max_len = 2)
            # print(frequent_item_set)
            df = pd.DataFrame(frequent_item_set)
            df = association_rules(df, metric="confidence", min_threshold = 0.15)
            df['Nconfidence'] = df['support'] / df['consequent support']
            df['data'] = dia.split('.')[0]
            df['loja'] = loja
            df['antecedents'] = df['antecedents'].apply(lambda a: tradutor[int(str(a).split('{')[1].split('}')[0])])
            df['consequents'] = df['consequents'].apply(lambda a: tradutor[int(str(a).split('{')[1].split('}')[0])])
            # df['antecedents'] = df['antecedents'].apply(lambda a: tradutor[a])
            # df['consequents'] = df['consequents'].apply(lambda a: tradutor[a])
            if len(totalDf) == 0:
                totalDf = df
            else:
                totalDf = totalDf.append(df)
            # print(totalDf)
            pass
if anual:
    totalDf.to_csv('SaidaAprioriAllLojasCMAnual.csv', sep=';')
else:
    totalDf.to_csv('SaidaAprioriAllLojasCM.csv', sep=';')

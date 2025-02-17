from mlxtend.preprocessing import TransactionEncoder
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import fpmax
from mlxtend.frequent_patterns import fpgrowth
from mlxtend.frequent_patterns import association_rules
from collections import defaultdict
import pandas as pd
import DatasetBuilder
import itertools
import pickle
import os
import randomCreator


def unfreeze(thing):
    item = str(thing)
    item = item.replace('[', '')
    item = item.replace(']', '')
    item = item.replace('{', '')
    item = item.replace('}', '')
    item = item.replace(')', '')
    item = item.replace('(', ' ')
    item = item.replace('frozenset', '')
    item = item.replace("'", '').strip()
    item = item.replace(',', ' ')
    return int(item)


pd.set_option('display.max_columns', 999)
pd.set_option('display.width', 999)


support_threshold = 0.0007
multiplicador_em_falha = 2
rand_support_threshold = 0.0007
confidence_threshold = 0.01

generate_natural = False
generate_random = False
generate_components = False
apriori_natural = True
apriori_random = True
incremet_random = True
split_date = False
generate_component_csv = False

DatasetBuilder.start(generate_natural, generate_random, generate_components)

if apriori_natural:
    print('generating natural')
    if not os.path.exists('output'):
        os.mkdir('output')
    for lojax in os.listdir('lojas_mensal'):
        loja = lojax.split('.')[0]
        print(loja)

        tradutor = randomCreator.read_pickle('tradutores/tradutor_loja_' + loja + '.pickle')
        component_dict = randomCreator.read_pickle('component_dict.pickle')
        loja_dict = pickle.load(open('lojas_mensal/' + loja + '.pickle', 'rb'))
        totalDf = pd.DataFrame()
        for mes in loja_dict.keys():
            print(mes)
            dataset = loja_dict[mes]
            te = TransactionEncoder()
            transaction_len = len(dataset)
            #  ___________________________________________________________________________________
            # debugs
            qtd_dict = defaultdict(int)
            for transaction in dataset:
                for iten in transaction:
                    qtd_dict[iten] += 1
            needed = support_threshold*transaction_len
            a = list(qtd_dict.keys())
            for key in a:
                if qtd_dict[key] < needed:
                    del qtd_dict[key]
            valids = list(qtd_dict.keys())
            new_dataset = []
            for transaction in dataset:
                new_transaction = []
                for item in transaction:
                    if item in valids:
                        new_transaction.append(item)
                if len(new_transaction) > 0:
                    new_dataset.append(new_transaction)
            dataset = new_dataset
            #  ___________________________________________________________________________________

            te_ary = te.fit(dataset).transform(dataset)
            df = pd.DataFrame(te_ary, columns=te.columns_)
            frequent_item_set = fpgrowth(df, min_support=support_threshold, use_colnames=True, max_len=2)
            # try:
            #     frequent_item_set = apriori(df, min_support = support_threshold, use_colnames = True,max_len=2)
            # except MemoryError:
            #     print('valor de ' + str(support_threshold) + ' nao funcionou incrementando para ' + str(support_threshold*multiplicador_em_falha) + ' no mes: ' + mes)
            #     frequent_item_set = apriori(df, min_support = support_threshold*multiplicador_em_falha, use_colnames = True, max_len = 2)
            df = pd.DataFrame(frequent_item_set)
            df = association_rules(df, metric="confidence", min_threshold = confidence_threshold)
            df['Nconfidence'] = df['support'] / df['consequent support']
            df['confidenceDif'] = abs(df['confidence'] - df['Nconfidence'])
            df['antecedents'] = df['antecedents'].apply(lambda a: tradutor[unfreeze(a)])
            df['consequents'] = df['consequents'].apply(lambda a: tradutor[unfreeze(a)])
            df["antecedent_margem"] = df["antecedents"].apply(lambda a: component_dict[loja][mes][a]['margem']/component_dict[loja][mes][a]['qtd'])
            df["antecedent_fat"] = df["antecedents"].apply(lambda a: component_dict[loja][mes][a]['faturamento']/component_dict[loja][mes][a]['qtd'])
            df["consequent_margem"] = df["consequents"].apply(lambda a: component_dict[loja][mes][a]['margem']/component_dict[loja][mes][a]['qtd'])
            df["consequent_fat"] = df["consequents"].apply(lambda a: component_dict[loja][mes][a]['faturamento']/component_dict[loja][mes][a]['qtd'])
            df["margem"] = (df["antecedent_margem"] + df["consequent_margem"]) * df["support"] * transaction_len
            df["faturamento"] = (df["antecedent_fat"] + df["consequent_fat"]) * df["support"] * transaction_len
            df['data'] = mes
            df['loja'] = loja
            df['qtd_cupons'] = transaction_len

            if len(totalDf) == 0:
                totalDf = df
            else:
                totalDf = totalDf.append(df)

        totalDf.to_csv('output/SaidaApriori_' + loja + '_CMMensal.csv', sep=';', index=False)

if apriori_random:
    print('generating random')
    if not os.path.exists('random_dicts'):
        os.mkdir('random_dicts')
    for lojax in os.listdir('random'):
        loja = lojax.split('.')[0]
        print(loja)
        tradutor = randomCreator.read_pickle('tradutores/tradutor_loja_' + loja + '.pickle')

        loja_dict = pickle.load(open('random/' + loja + '.pickle', 'rb'))
        totalDf = pd.DataFrame()
        compare_dict = {}
        for mes in loja_dict.keys():
            print(mes)
            dataset = loja_dict[mes]
            transaction_len = len(dataset)
            #  ___________________________________________________________________________________
            # debugs
            qtd_dict = defaultdict(int)
            for transaction in dataset:
                for iten in transaction:
                    qtd_dict[iten] += 1
            needed = support_threshold * transaction_len
            a = list(qtd_dict.keys())
            for key in a:
                if qtd_dict[key] < needed:
                    del qtd_dict[key]
            valids = list(qtd_dict.keys())
            new_dataset = []
            for transaction in dataset:
                new_transaction = []
                for item in transaction:
                    if item in valids:
                        new_transaction.append(item)
                if len(new_transaction) > 0:
                    new_dataset.append(new_transaction)
            dataset = new_dataset
            #  ___________________________________________________________________________________
            te = TransactionEncoder()
            te_ary = te.fit(dataset).transform(dataset)
            randf = pd.DataFrame(te_ary, columns=te.columns_)
            frequent_item_set = fpgrowth(randf, min_support=support_threshold, use_colnames=True, max_len=2)
            # try:
            #     frequent_item_set = apriori(randf, min_support = rand_support_threshold, use_colnames = True, max_len = 2)
            # except MemoryError:
            #     print('valor de ' + str(rand_support_threshold) + ' nao funcionou incrementando para '+str(rand_support_threshold*multiplicador_em_falha)+' no mes: ' + mes)
            #     frequent_item_set = apriori(randf, min_support = rand_support_threshold*multiplicador_em_falha, use_colnames = True, max_len = 2)
            randf = pd.DataFrame(frequent_item_set)
            randf = association_rules(randf, metric="confidence", min_threshold =confidence_threshold)
            randf['Nconfidence'] = randf['support'] / randf['consequent support']
            randf['antecedents'] = randf['antecedents'].apply(lambda a: tradutor[int(str(a).split('{')[1].split('}')[0])])
            randf['consequents'] = randf['consequents'].apply(lambda a: tradutor[int(str(a).split('{')[1].split('}')[0])])
            for i, line in randf.iterrows():
                antecedent = line['antecedents']
                consequent = line['consequents']
                confidence = line['confidence']
                nconfidence = line['Nconfidence']
                if antecedent not in compare_dict.keys():
                    compare_dict[antecedent] = {}
                if consequent not in compare_dict.keys():
                    compare_dict[consequent] = {}
                if consequent not in compare_dict[antecedent].keys():
                    compare_dict[antecedent][consequent] = confidence
                if antecedent not in compare_dict[consequent].keys():
                    compare_dict[consequent][antecedent] = nconfidence

        randomCreator.savepickle(compare_dict, 'random_dicts/' + loja + '.pickle')

if incremet_random:
    print('increment random')
    totalDf = pd.DataFrame()
    for file in os.listdir('output'):
        loja = file.split('SaidaApriori_')[1].split('_CMMensal.csv')[0]
        print(loja)
        compare_dict = randomCreator.read_pickle('random_dicts/' + loja + '.pickle')
        df = pd.read_csv('output/' + file, sep=';')

        list_of_confidence = []
        for i, line in df.iterrows():
            antecedent = line['antecedents']
            consequent = line['consequents']
            if antecedent in compare_dict.keys():
                if consequent in compare_dict[antecedent].keys():
                    list_of_confidence.append(compare_dict[antecedent][consequent])
                    continue
            list_of_confidence.append(0.0)
        df['confidenceRand'] = list_of_confidence
        df['conf{real - random}'] = df['confidence'] - df['confidenceRand']
        df['association'] = df['antecedents'] + ' -> ' + df['consequents']
        df = df.drop(columns = ["confidenceRand"])
        if len(totalDf) == 0:
            totalDf = df
        else:
            totalDf = totalDf.append(df)

    totalDf.to_csv('saidaall.csv', sep = ';', index = False)

if split_date:
    saida = pd.read_csv('saidaall.csv', sep = ';')
    if 'association' not in saida.columns:
        saida['association'] = saida['antecedents'] + ' -> ' + saida['consequents']
    for loja in saida['loja'].unique():
        tempdf = saida[saida['loja'] == loja]

        associations = tempdf['association'].unique()
        meses = tempdf['data'].unique()
        newindex =list(itertools.product(associations, meses))
        tempdf.set_index(['association', 'data'], inplace = True)
        tempdf = tempdf[~tempdf.index.duplicated()]
        tempdf = tempdf.reindex(newindex)
        tempdf.reset_index(inplace = True)


        tempdf.to_csv('saidaall_' + str(loja) + '.csv', sep=';',index = False)

if generate_component_csv:
    component_dict = randomCreator.read_pickle('component_dict.pickle')
    lojalist = []
    datalist = []
    itemlist = []
    qtdlist = []
    fatlist = []
    marlist = []
    for loja in component_dict.keys():
        for data in component_dict[loja].keys():
            for produto in component_dict[loja][data].keys():
                qtdlist.append(component_dict[loja][data][produto]['qtd'])
                marlist.append(component_dict[loja][data][produto]['margem'])
                fatlist.append(component_dict[loja][data][produto]['faturamento'])
                lojalist.append(loja)
                itemlist.append(produto)
                datalist.append(data)
    df = pd.DataFrame()
    df['loja'] = lojalist
    df['faturamento'] = fatlist
    df['data'] = datalist
    df['margem'] = marlist
    df['produto'] = itemlist
    df['qtd'] = qtdlist
    df.to_csv('component_all.csv',sep=';',index = False,encoding = 'utf-8')

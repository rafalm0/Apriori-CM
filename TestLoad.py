import pandas as pd
import pickle
import os
import datetime
import copy
from random import shuffle
import aaa


def defmon(string):
    if '-' in string:
        x = int(string.split('-')[1])
        if x % 2 > 0:
            return str(x)
        else:
            return str(x-1)
    else:
        if int(string) % 2 > 0:
            return string
        else:
            return str(int(string) - 1)


def start():
    generate = True
    if not os.path.exists('tradutores'):
        os.mkdir('tradutores')
    if not os.path.exists('lojas_mensal'):
        os.mkdir('lojas_mensal')

    component_dict_associations = {}
    if generate:
        for file in os.listdir('cupons'):
            dict_traduto = {}
            print(file)
            df = pd.read_csv('cupons/' + file, sep=',', encoding='utf-8', usecols=['produtoid', 'produto', 'data', 'vendaid', 'produto', 'quantidade', 'valortotal', 'custo'])
            df = df.sort_values(by = ['data', 'vendaid'])
            # df['data'] = df['data'].apply(lambda a: str(int(a)-1) if int(a)%2>0 else a)
            loja = '_'.join(file.split('_')[1:3])
            ultimo_cupom = None

            dataset = []
            cupom = []
            ultima_data = None
            datasets_dia = {}

            for i, line in df.iterrows():
                if ultimo_cupom is None:
                    ultimo_cupom = line['vendaid']
                cupom_atual = line['vendaid']

                if ultima_data is None:
                    ultima_data = defmon(line['data'])
                data_atual = defmon(line['data'])

                faturamentototal = line['valortotal']
                margemtotal = line['valortotal'] - (line['custo']*line['quantidade'])
                quantidade = line['quantidade']

                dict_traduto[line['produtoid']] = line['produto']

                if loja not in component_dict_associations.keys():
                    component_dict_associations[loja] = {}
                if data_atual not in component_dict_associations[loja].keys():
                    component_dict_associations[loja][data_atual] = {}
                if line['produto'] not in component_dict_associations[loja][data_atual].keys():
                    component_dict_associations[loja][data_atual][line['produto']] = {'qtd': 0, 'margem': 0, 'faturamento': 0}
                component_dict_associations[loja][data_atual][line['produto']]['qtd'] += quantidade
                component_dict_associations[loja][data_atual][line['produto']]['margem'] += margemtotal
                component_dict_associations[loja][data_atual][line['produto']]['faturamento'] += faturamentototal

                if cupom_atual != ultimo_cupom:
                    dataset.append(copy.deepcopy(cupom))
                    cupom = []

                if data_atual != ultima_data:
                    datasets_dia[ultima_data] = dataset
                    dataset = []

                cupom.append(line['produtoid'])
                ultimo_cupom = line['vendaid']
                ultima_data = data_atual

            dataset.append(cupom)
            datasets_dia[ultima_data] = dataset
            pickle.dump(dict_traduto, open('tradutores/tradutor_loja_' + loja + '.pickle', 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
            pickle.dump(datasets_dia, open('lojas_mensal/' + loja + '.pickle', 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
        pickle.dump(component_dict_associations, open('component_dict.pickle', 'wb'), protocol=pickle.HIGHEST_PROTOCOL)

    files = os.listdir('lojas_mensal')
    for file in files:
        aaa.create_random('lojas_mensal/' + file)



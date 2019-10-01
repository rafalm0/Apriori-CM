import pandas as pd
import pickle
import os
import datetime
import copy
from random import shuffle
import randomCreator


def defmon(string):
    x = int(string.split('-')[1])
    if x % 2 > 0:
        return str(x)
    else:
        return str(x-1)


def start(cupons = True, random = True,component = True):
    if not os.path.exists('tradutores'):
        os.mkdir('tradutores')
    if not os.path.exists('lojas_mensal'):
        os.mkdir('lojas_mensal')
    if not os.path.exists('random'):
        os.mkdir('random')

    component_dict_associations = {}
    if component or cupons:
        for file in os.listdir('cupons'):
            dict_traduto = {}
            print(file)
            df = pd.read_csv('cupons/' + file, sep=',', encoding='utf-8', usecols=['produtoid', 'produto', 'data', 'vendaid', 'produto', 'quantidade', 'valortotal', 'custo'])
            df = df.sort_values(by = ['data', 'vendaid'])
            df['mes'] = df['data'].apply(lambda a: defmon(a))
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
                    ultima_data = line['mes']
                data_atual = line['mes']

                faturamentototal = line['valortotal']
                custo = line['custo']
                if custo != custo:
                    margemtotal = 0
                else:
                    margemtotal = line['valortotal'] - (custo*line['quantidade'])
                quantidade = line['quantidade']

                dict_traduto[line['produtoid']] = line['produto']
                if component:
                    if loja not in component_dict_associations.keys():
                        component_dict_associations[loja] = {}
                    if data_atual not in component_dict_associations[loja].keys():
                        component_dict_associations[loja][data_atual] = {}
                    if line['produto'] not in component_dict_associations[loja][data_atual].keys():
                        component_dict_associations[loja][data_atual][line['produto']] = {'qtd': 0, 'margem': 0, 'faturamento': 0}
                    component_dict_associations[loja][data_atual][line['produto']]['qtd'] += quantidade
                    if margemtotal == margemtotal:
                        component_dict_associations[loja][data_atual][line['produto']]['margem'] += margemtotal
                    component_dict_associations[loja][data_atual][line['produto']]['faturamento'] += faturamentototal
                if cupons:
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
            if cupons:
                pickle.dump(datasets_dia, open('lojas_mensal/' + loja + '.pickle', 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
        if component:
            pickle.dump(component_dict_associations, open('component_dict.pickle', 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
    if random:
        files = os.listdir('lojas_mensal')
        for file in files:
            randomCreator.create_random('lojas_mensal/' + file)



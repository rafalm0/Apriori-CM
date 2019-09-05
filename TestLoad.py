import pandas as pd
import pickle
import os


anual = True
if not os.path.exists('tradutores'):
    os.mkdir('tradutores')
if not os.path.exists('lojas_anual'):
    os.mkdir('lojas_anual')
for file in os.listdir('cupons'):
    dict_traduto = {}
    print(file)
    df = pd.read_csv('cupons/' + file, sep=',', encoding='utf-8', usecols=['produtoid', 'data', 'vendaid','produto'])
    loja = '_'.join(file.split('_')[1:3])
    if not os.path.exists('lojas/' + loja):
        os.mkdir('lojas/' + loja)
    if not os.path.exists('lojas_anual/' + loja):
        os.mkdir('lojas_anual/' + loja)
    data = ''
    ultimo_cupom = ''
    dataset = []
    cupom = []
    for i, line in df.iterrows():
        dict_traduto[line['produtoid']] = line['produto']
        if line['vendaid'] != ultimo_cupom:
            if ultimo_cupom != '':

                dataset.append(cupom)
                cupom = []
            ultimo_cupom = line['vendaid']
        if not anual:
            if line['data'] != data:
                if data != '':
                    dataset.append(cupom)
                    cupom = []
                    pickle.dump(dataset, open('lojas/' + loja + '/' + str(data) + '.pickle', 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
                    dataset = []
                data = line['data']
        if line['produtoid'] not in cupom:
            cupom.append(line['produtoid'])
    dataset.append(cupom)

    # pickle.dump(dict_traduto, open('tradutores/tradutor_loja_' + loja + '.pickle', 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
    if anual:
        pickle.dump(dataset, open('lojas_anual/' + loja + '.pickle', 'wb'), protocol=pickle.HIGHEST_PROTOCOL)
    else:
        pickle.dump(dataset, open('lojas/' + loja + '/' + str(data) + '.pickle', 'wb'), protocol=pickle.HIGHEST_PROTOCOL)

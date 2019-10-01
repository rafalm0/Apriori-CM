import pickle
from random import shuffle
import os


def read_pickle(path):
    return pickle.load(open(path, 'rb'))


def savepickle(objeto, path):
    pickle.dump(objeto, open(path, 'wb'), protocol = pickle.HIGHEST_PROTOCOL)
    return


def create_random(path):
    path_saida = 'random/' + path.split('.')[0].split('/')[1] + '.pickle'
    datasetx = read_pickle(path)
    random_dict = {}
    sacola_itens = []

    for mes in datasetx.keys():
        tamanhos = []
        for transaction in datasetx[mes]:
            tamanho = len(transaction)
            tamanhos.append(tamanho)
            for item in transaction:
                sacola_itens.append(item)
        shuffle(sacola_itens)
        random_dataset = []
        for tamanho in tamanhos:
            random_transaction = []
            for item in range(0, tamanho):
                random_transaction.append(sacola_itens.pop())
            random_dataset.append(random_transaction)
        random_dict[mes] = random_dataset
    savepickle(random_dict, path_saida)
    return


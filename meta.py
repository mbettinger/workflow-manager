import pickle as pk
from workflow_manager.data import Data
from workflow_manager.temporizer import *

def sort(data_containers, criterion,asc=False):
    return sorted(data_containers,key=criterion)[::2*asc-1]

def top_N(data_containers, criterion, n_best):
    return sort(data_containers, criterion)[:min(n_best, len(data_containers))]

def conditional_removal(data_containers, condition, keep=False):
    return [container for container in data_containers if keep and condition(container) or (not keep and not condition(container))]

def import_data(data_containers, import_path, pickle=True):
    if pickle:
        with open(import_path, "rb") as file:
            data_containers.extend(pk.Unpickler(file).load())
    else:
        raise NotImplementedError()
    return data_containers

def export_data(data_containers, export_path, pickle=True):
    if pickle:
        with open(export_path, "wb") as file:
            pk.Pickler(file).dump(data_containers)
    else:
        with open(export_path, "w") as file:
            file.write(str(data_containers))
    return data_containers

def split(data_containers, key):
    newDataContainers=[]
    for data_c in data_containers:
        for val in exec_just_in_time(data_c[key]):
            dc=Data.rel_deep_copy(data_c)
            dc[key]=val
            newDataContainers.append(dc)
    return newDataContainers

def remove_params(data_containers, keys):
    for dc in data_containers:
        for key in keys:
            if key in dc.keys():
                dc.pop(key)
    return data_containers
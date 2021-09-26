import argparse
import sys
import json

import numpy as np
import pandas as pd


def define_recorded_indices(thread):
    """Define nodes that were already included in threads."""
    recorded_indices = []
    for i in thread:
        for j in i:
            if type(j) != int:
                recorded_indices = recorded_indices + j
            else:
                recorded_indices = recorded_indices + [j]
    recorded_indices_wo_nan = [index for index in recorded_indices if np.isnan(index) != True]
    return recorded_indices_wo_nan


def define_indices_w_multi(edge_list, nodes):
    """Defines indices of that have either multiple target nodes or multiple
    source nodes."""
    if nodes == 'target':
        n_nodes = pd.DataFrame(edge_list['id_source'].value_counts())
        n_nodes['id'] = n_nodes.index
        multiple_nodes_id = pd.to_numeric(n_nodes['id'][n_nodes['id_source'] > 1])
    elif nodes == 'source':
        n_nodes = pd.DataFrame(edge_list['id_target'].value_counts())
        n_nodes['id'] = n_nodes.index
        multiple_nodes_id = pd.to_numeric(n_nodes['id'][n_nodes['id_target'] > 1])
    else:
        raise ValueError('Nodes parameter can be either "target" or "source".')
    multi_id = multiple_nodes_id.values.tolist()
    return multi_id


def define_indices_w_multi_srcs_trgts(edge_list):
    """Defines nodes that are contained in complex threads."""
    multi_srcs_id = define_indices_w_multi(edge_list, 'source')
    multi_trgts_id = define_indices_w_multi(edge_list, 'target')
    multi_srcs_trgts_id = []
    for i in multi_srcs_id:
        for j in multi_trgts_id:
            if i == j:
                multi_srcs_trgts_id = multi_srcs_trgts_id + [i]
    for i in multi_srcs_trgts_id:
        multi_srcs_id.remove(i)
        multi_trgts_id.remove(i)
    return multi_srcs_id, multi_trgts_id, multi_srcs_trgts_id


def parse_multi_target_source_thread(ind, mat):
    """Extracts nodes that have multiple source nodes and multiple target nodes
    in the same thread."""
    complex_thread_sources_targets = []
    cts = []
    ctt = []
    cts = [mat['id_source'][mat['id_target'] == ind].values.tolist()]
    ctt = [mat['id_target'][mat['id_source'] == ind].values.tolist()]
    complex_thread_sources_targets = complex_thread_sources_targets+cts + [ind]+ctt
    return complex_thread_sources_targets


def extract_very_complex_thread(multi_srcs_trgts_id, mat):
    """Attaches two sets of nodes to nodes that have multiple source nodes and multiple target nodes: target
    nodes and source nodes. These short threads are further store in a list of lists."""
    list_src_trgt_thrd = []
    for ind in multi_srcs_trgts_id:
        complex_thread_sources_targets = parse_multi_target_source_thread(ind, mat)
        list_src_trgt_thrd = list_src_trgt_thrd + [complex_thread_sources_targets]
    return list_src_trgt_thrd


def parse_srcs_very_complex_thread(complex_thread_sources_targets, mat):
    """Attaches source nodes to a short thread that has a node that has multiple source nodes and multiple
    target nodes."""
    sources = []
    for i in complex_thread_sources_targets[0]:
        j = 0
        while j < 60:
            if i == mat[j][1]:
                sources = sources + [mat[j][0]]
            j += 1
    if 1 <= len(sources) < len(complex_thread_sources_targets[0]):
        i = 1
        while i < len(complex_thread_sources_targets[0]) - len(sources) + 1:
            sources.append(np.nan)
            i += 1
    if sources:
        complex_thread_sources_targets = [sources] + complex_thread_sources_targets
    return complex_thread_sources_targets


def iterate_srcs_cmplx_thrd(list_src_trgt_thrd, mat):
    """Iterates over a list of nodes that have multiple target nodes and multiple source nodes and attaches
    source nodes to these nodes."""
    matrix_list = mat.values.tolist()
    i = 0
    while i < len(list_src_trgt_thrd):
        list_src_trgt_thrd[i] = parse_srcs_very_complex_thread(list_src_trgt_thrd[i], matrix_list)
        if len(parse_srcs_very_complex_thread(list_src_trgt_thrd[i], matrix_list)) == len(list_src_trgt_thrd[i]):
            i += 1
    return list_src_trgt_thrd


def parse_trgts_very_complex_thread(complex_thread_sources_targets, mat):
    """Attaches target nodes to a short thread that has a node that has multiple source nodes and multiple
    target nodes."""
    targets = []
    for i in complex_thread_sources_targets[-1]:
        j = 0
        while j < 60:
            if i == mat[j][0]:
                targets = targets + [mat[j][1]]
            j += 1
    if 1 <= len(targets) < len(complex_thread_sources_targets[-1]):
        i = 1
        while i < len(complex_thread_sources_targets[-1]) - len(targets) + 1:
            targets.append(np.nan)
            i += 1
    if targets:
        complex_thread_sources_targets = complex_thread_sources_targets + [targets]
    return complex_thread_sources_targets


def iterate_trgts_cmplx_thrd(list_src_trgt_thrd, mat):
    """Iterates over a list of nodes that have multiple target nodes and multiple source nodes and attaches
    target nodes to these nodes."""
    matrix_list = mat.values.tolist()
    i = 0
    while i < len(list_src_trgt_thrd):
        list_src_trgt_thrd[i] = parse_trgts_very_complex_thread(list_src_trgt_thrd[i], matrix_list)
        if len(parse_trgts_very_complex_thread(list_src_trgt_thrd[i], matrix_list)) == len(list_src_trgt_thrd[i]):
            i += 1
    return list_src_trgt_thrd


def get_threads_multi_sources_multi_targets(mat, multi_srcs_trgts_id=[]):
    """Extracts the most complex threads in a network."""
    if multi_srcs_trgts_id:
        list_src_trgt_thrd = extract_very_complex_thread(multi_srcs_trgts_id, mat)
        list_src_trgt_thrd = iterate_srcs_cmplx_thrd(list_src_trgt_thrd, mat)
        list_src_trgt_thrd = iterate_trgts_cmplx_thrd(list_src_trgt_thrd, mat)
        recorded_indices_cmplx_thrd = define_recorded_indices(list_src_trgt_thrd)
        return list_src_trgt_thrd, recorded_indices_cmplx_thrd


def parse_multi_source_thread(ind, mat):
    cts = []
    cts = [[mat['id_source'][mat['id_target'] == ind].values.tolist()] + [ind]]
    return cts


def extract_complex_thread_sources(multi_srcs_id, mat):
    list_srcs_thrd = []
    for ind in multi_srcs_id:
        complex_thread_sources = parse_multi_source_thread(ind, mat)
        list_srcs_thrd = list_srcs_thrd + complex_thread_sources
    return list_srcs_thrd


def parse_trgt_complex_thread_sources(complex_thread_sources, mat):
    targets = []
    i = complex_thread_sources[-1]
    j = 0
    while j < len(mat):
        if i == mat[j][0]:
            targets = targets + [mat[j][1]]
        j += 1
    if targets:
        complex_thread_sources = complex_thread_sources + targets
    return complex_thread_sources


def iterate_trgt_thrd_sources(list_srcs_thrd, mat):
    matrix_list = mat.values.tolist()
    i = 0
    while i < len(list_srcs_thrd):
        list_srcs_thrd[i] = parse_trgt_complex_thread_sources(list_srcs_thrd[i], matrix_list)
        if len(parse_trgt_complex_thread_sources(list_srcs_thrd[i], matrix_list)) == len(list_srcs_thrd[i]):
            i += 1
    return list_srcs_thrd


def get_threads_multi_sources(mat, multi_srcs_id, recorded_indices_cmplx_thrd=[]):
    """Extracts threads that have nodes that have multiple source nodes in a thread."""
    if multi_srcs_id:
        multi_srcs_id = [i for i in multi_srcs_id if i not in recorded_indices_cmplx_thrd]
    if multi_srcs_id:
        list_srcs_thrd = extract_complex_thread_sources(multi_srcs_id, mat)
        list_srcs_thrd = iterate_trgt_thrd_sources(list_srcs_thrd, mat)
        list_srcs_thrd = iterate_srcs_cmplx_thrd(list_srcs_thrd, mat)
        recorded_indices_w_multi_srcs = define_recorded_indices(list_srcs_thrd)
        recorded_indices_wo_nan = recorded_indices_cmplx_thrd + recorded_indices_w_multi_srcs
        return list_srcs_thrd, recorded_indices_wo_nan


def parse_multi_target_thread(ind, mat):
    """Attaches target nodes to a node that has multiple target nodes."""
    ctt = []
    complex_thread_trgts = []
    ctt = [ind] + [mat['id_target'][mat['id_source'] == ind].values.tolist()]
    complex_thread_trgts = complex_thread_trgts + [ctt]
    return complex_thread_trgts


def extract_complex_thread_targets(multi_trgts_id, mat):
    """Iterates over the list of nodes that have multiple target nodes and attaches multiple target nodes to the
    nodes. These lists are further stored in a list of lists."""
    list_trgt_thrd = []
    for ind in multi_trgts_id:
        complex_thread_targets = parse_multi_target_thread(ind, mat)
        list_trgt_thrd = list_trgt_thrd + complex_thread_targets
    return list_trgt_thrd


def parse_srcs_complex_thread_targets(complex_thread_sources_targets, mat):
    """Attaches source nodes to a thread of a node that has multiple target nodes"""
    sources = []
    i = complex_thread_sources_targets[0]
    j = 0
    while j < 60:
        if i == mat[j][1]:
            sources = sources + [mat[j][0]]
        j += 1
    if sources:
        complex_thread_sources_targets = sources + complex_thread_sources_targets
    return complex_thread_sources_targets


def iterate_srcs_thrd_targets(list_trgt_thrd, mat):
    """Iterates over the list of nodes that have multiple target nodes and attaches
    source nodes to these nodes"""
    matrix_list = mat.values.tolist()
    i = 0
    while i < len(list_trgt_thrd):
        list_trgt_thrd[i] = parse_srcs_complex_thread_targets(list_trgt_thrd[i], matrix_list)
        if len(parse_srcs_complex_thread_targets(list_trgt_thrd[i], matrix_list)) == len(list_trgt_thrd[i]):
            i += 1
    return list_trgt_thrd


def get_threads_multi_targets(mat, multi_trgts_id, recorded_indices_wo_nan=[]):
    """Extracts threads that have nodes that have multiple target nodes in a thread."""
    multi_trgts_id = [i for i in multi_trgts_id if i not in recorded_indices_wo_nan]
    if multi_trgts_id:
        list_trgt_thrd = extract_complex_thread_targets(multi_trgts_id, mat)
        list_trgt_thrd = iterate_srcs_thrd_targets(list_trgt_thrd, mat)
        list_trgt_thrd = iterate_trgts_cmplx_thrd(list_trgt_thrd, mat)
        recorded_indices_w_multi_trgts = define_recorded_indices(list_trgt_thrd)
        recorded_indices_wo_nan = recorded_indices_wo_nan + recorded_indices_w_multi_trgts
        return list_trgt_thrd, recorded_indices_wo_nan


def get_simple_threads(mat, recorded_indices_wo_nan=[]):
    """Extracts simple threads."""
    indices = (mat['id_source'].append(mat['id_target'])).unique().tolist()
    left_indices = [i for i in indices if i not in recorded_indices_wo_nan]
    if left_indices:
        mat_list = mat.values.tolist()
        i = 0
        threads = []
        recorded_indices = []
        while i < len(left_indices):
            if left_indices[i] not in recorded_indices:
                index = left_indices[i]
                x = 0
                while x < len(mat_list):
                    if type(index) == int:
                        if mat_list[x][0] == index:
                            index = [index] + [mat_list[x][1]]
                            x += 1
                    if type(index) == list:
                        if mat_list[x][0] == index[-1]:
                            index = index + [mat_list[x][1]]
                    x += 1
                threads = threads + [index]
                recorded_indices = recorded_indices + index
            i += 1
        recorded_indices_wo_nan = recorded_indices_wo_nan + recorded_indices
        return threads, recorded_indices_wo_nan


def construct_threads(edge_list):
    mat = edge_list.drop(labels='Unnamed: 0', axis=1)[['id_source', 'id_target']]
    multi_srcs_id, multi_trgts_id, multi_srcs_trgts_id = define_indices_w_multi_srcs_trgts(edge_list)
    if multi_srcs_trgts_id:
        list_srcs_trgts_thrd, recorded_indices = get_threads_multi_sources_multi_targets(mat, multi_srcs_trgts_id)
    else:
        list_srcs_trgts_thrd = []
        print("""There aren't nodes that have multiple target nodes and multiple source nodes.""")
    if multi_srcs_id:
        list_srcs_thrd, recorded_indices = get_threads_multi_sources(mat, multi_srcs_id, recorded_indices)
    else:
        list_srcs_thrd = []
        print("""There aren't nodes that have multiple source nodes: these do not exist in the network or were
                        already recorded as nodes that have multiple target nodes and multiple source nodes as
                        well.\n""")
    if multi_trgts_id:
        list_trgts_thrd, recorded_indices = get_threads_multi_targets(mat, multi_trgts_id, recorded_indices)
    else:
        list_trgts_thrd = []
        print("""There aren't nodes that have multiple target nodes: these do not exist in the network or were
                already recorded as nodes that have multiple target nodes and multiple source nodes as well.\n""")
    simple_threads, recorded_indices = get_simple_threads(mat, recorded_indices)
    if set((mat['id_source'].append(mat['id_target'])).unique().tolist()) == set(recorded_indices):
        network = []
        for i in list_srcs_trgts_thrd, list_srcs_thrd, list_trgts_thrd, simple_threads:
            if i:
                network = network + i
        return network
    else:
        raise ValueError("""Number of nodes included in threads is not equal to number of nodes that a network
        contains.\n""")


def parse_arguments():
    parser = argparse.ArgumentParser(description="""Constructs threads that are contained in a river network.""")
    parser.add_argument('in_file', nargs='?', help='Input file (csv)', type=argparse.FileType('r'),
                        default=sys.stdin)
    parser.add_argument('out_file', nargs='?', help='Output file (json)', action='store')
    return parser.parse_args()


def main():
    args = parse_arguments()
    filtered_metrics = pd.read_csv(args.in_file)
    network = construct_threads(filtered_metrics)
    with open(args.out_file, 'w') as f:
        json.dump(network, f, indent=2)


if __name__ == '__main__':
    main()

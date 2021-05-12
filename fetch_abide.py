'''

This function is used to fetch ABIDE I and ABIDE II fMRI or MRI data.
Preprocessed ABIDE I is available.

'''
import requests
import os
import io
import pandas as pd
import numpy as np
from nilearn.datasets import fetch_abide_pcp
from nilearn.datasets.utils import _fetch_file
import argparse


def fetch_abide(root_dir = './', data_type = 'func', preproc_ii = False,
                band_pass_filtering = False, global_signal_regression = False,
                pipeline = 'cpac', verbose = 0):
    # Fetch ABIDE I
    # Since preprocessed data fMRI is available
    # fetch func_preproc directly.
    abide1_type = [data_type + '_preproc']
    abide1 = fetch_abide_pcp(data_dir = root_dir, derivatives = abide1_type, verbose = verbose,
                    pipeline = pipeline, band_pass_filtering = band_pass_filtering,
                    global_signal_regression = global_signal_regression)

    abide1_num = len(abide1['func_preproc'])
    # Get the ABIDE I path
    strategy = ''
    if not band_pass_filtering:
        strategy += 'no'
    strategy += 'filt_'
    if not global_signal_regression:
        strategy += 'no'
    strategy += 'global'

    data_folder = 'ABIDE_pcp'
    path = os.path.join(root_dir, data_folder, pipeline, strategy)

    # Fetch ABIDE II
    # preprocessed fMRI is not available ATM
    # fetch raw fMRI instead
    base_url = 'https://s3.amazonaws.com/fcp-indi/data/Projects/ABIDE2/RawData/'

    ins = ['USM_1', 'UCLA_1', 'UCD_1', 'TCD_1', 'SDSU_1',
           'ONRC_2', 'OHSU_1', 'NYU_2', 'NYU_1', 'KUL_3',
           'KKI_1', 'IU_1', 'IP_1', 'GU_1', 'ETHZ_1',
           'EMC_1', 'BNI_1']

    phenotypic = pd.DataFrame()

    if not preproc_ii:
        abide2_num = 0
        for enum in ins:
            child_base = base_url + 'ABIDEII-' + enum
            file_name = '/participants.tsv'
            child_url = child_base + file_name

            # Some folders may not contain the phenotypic data
            # Example file path: https://s3.amazonaws.com/fcp-indi/data/Projects/
            #                          ABIDE2/RawData/ABIDEII-USM_1/sub-29527/ses-1/
            #                          func/sub-29527_ses-1_task-rest_run-1_bold.nii.gz
            if requests.get(child_url).status_code == 404:
                print('Phenotypic data of %s not found !!!' % enum)
                print('Expected url: %s' % child_url)
                continue

            print('Successfully get the phenotypic data of %s' % enum)

            temp_table = pd.read_csv(io.StringIO(requests.get(child_url).text), sep='\t')
            phenotypic = pd.concat([phenotypic, temp_table])

            ids = temp_table['participant_id']
            print('%d samples to download in %s dataset' % (len(ids.values), enum))
            for id in ids:
                child_path = '/sub-' + str(id) + '/ses-1/func'
                file_name = '/sub-' + str(id) + '_ses-1_task-rest_run-1_bold.nii.gz'
                child_url = child_base + child_path + file_name

                # in case the file name is wrong
                if requests.get(child_url).status_code == 404:
                    print('No such file: %s !!!' % child_url)
                    continue

                abide2_num += 1
                file_name = child_url.split('/')[-1]
                file_path = os.path.join(path, file_name)

                # if the file is already downloaded, do nothing
                if os.path.exists(file_path):
                    continue

                _fetch_file(child_url, file_path, verbose=verbose)

    phenotypic.to_csv(os.path.join(root_dir, data_folder, 'Phenotypic_ABIDE2.csv'))

    return abide1_num, abide2_num


def main(args):
    fetch_abide(root_dir=args.data_dir, verbose=1)


if __name__ == '__main__':
    parse = argparse.ArgumentParser()
    parse.add_argument('data_dir', default='./', help='Path to store the data')
    args = parse.parse_args()

    main(args)
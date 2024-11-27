import json
import os

import h5py
import numpy as np
import tqdm
import load_h5


def add_to_hdf5(group, path, update_groups=[]):
    if len(update_groups) == 0: 
        print('No new group specified')
        return
    for item in tqdm.tqdm(os.listdir(path)):
        file_path = os.path.join(path, item)

        if file_path.count("/") >= 2 and not any([g in file_path.split("/") for g in update_groups]):
            print('Skipping', file_path)
            continue
        # print('Adding', file_path)

        if os.path.isdir(file_path):
            # Create a new group for the directory
            try:
                sub_group = group.create_group(item)
            except ValueError:
                # delete the existing group and create a new one
                del group[item]
                sub_group = group.create_group(item)
            
            add_to_hdf5(sub_group, file_path, update_groups)
        else:
            data = None
            # Create a dataset in the current group for the file
            if file_path.endswith('.jpg') or file_path.endswith('.png') or file_path.endswith('.gif'):
                # Process as binary data
                with open(file_path, 'rb') as f:
                    data = f.read()
            elif file_path.endswith('.json'):
                # Process as JSON
                with open(file_path, 'r') as f:
                    json_data = json.load(f)
                # Convert to string for storage
                json_str = json.dumps(json_data)
                data = json_str.encode('utf-8')
            elif file_path.endswith('.txt'):
                with open(file_path, 'r') as f:
                    txt_data = f.read()
                data = txt_data.encode('utf-8')
            elif file_path.endswith('.csv'):
                with open(file_path, 'r') as f:
                    csv_data = f.read()
                data = csv_data.encode('utf-8')        
            elif file_path.endswith('.mp4') or file_path.endswith('.avi') or file_path.endswith('.mov'):
                with open(file_path, 'rb') as f:
                    data = f.read()
            elif file_path.endswith('.webm'):
                with open(file_path, 'rb') as f:
                    data = f.read()
                
            if data is None:
                print(f"No matched data type for {file_path}")
                continue

            try:
                group.create_dataset(item, data=np.array(data, dtype='S'))
            except ValueError:
                del group[item]
                group.create_dataset(item, data=np.array(data, dtype='S'))


if __name__ == '__main__':
    ################# Modify this section ####################
    # base path
    base_path = "/home/135/qc2666/dg/datasets/kinetics-dataset/k700-2020"
    # dataset path
    dataset_path = os.path.join(base_path, 'test')
    # hdf5 file path
    save_path = os.path.join(base_path, 'k700_2020_test.h5')

    """
        [DEFAULT]: update_groups = []
        Nothing will be appended
        or
        List file/folder to be appended:
        update_groups = ["dir1", "file1"]
        or
        ALERT: every file will be rewritten!
        update_groups = [""] 
    """
    # update_groups = ["raw_video", "train.csv", "test.csv"]
    update_groups = ["Kinetics700-2020-test"]
    ##########################################################

    if not os.path.exists(save_path):
        hf = h5py.File(save_path, 'w')
    else:
        hf = h5py.File(save_path, 'a')
    
    
    add_to_hdf5(hf, dataset_path, update_groups)

    # group check
    load_h5.get_h5_struct(save_path, update=True)

    # # load the file
    # csv = load_h5.load_h5_file(hf, 'test.csv')
    # for clip_idx, path_label in enumerate(csv.split("\n")):
    #     print(clip_idx, path_label)

    hf.close()
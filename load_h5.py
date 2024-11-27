import h5py
import numpy as np
import os
import io
from PIL import Image
import json


def get_h5_struct(path, update=False):
    data = []
    group = []
    hf = h5py.File(path, 'r')

    # Store the indexed struct into json to save processing time
    cache_path = os.path.join(os.path.splitext(path)[0] + "_struct_tmp.json")
    print(f"check cache_path: {cache_path}")

    if os.path.exists(cache_path) and not update:
        print('Loading cached struct json file...')
        with open(cache_path, 'r') as f:
            struct = json.load(f)
    else:
        print('Indexing h5 file...')
        def visit_h5_file(name, obj):
            if isinstance(obj, h5py.Dataset):
                data.append(name)
            elif isinstance(obj, h5py.Group):
                group.append(name)
            else:
                raise ValueError('Unknown type: {}'.format(name))
            
        hf.visititems(visit_h5_file)
        
        struct = {g: [] for g in group}
        
        for d in data:
            group_name = d.rsplit('/', 1)[0]
            if group_name in struct:
                struct[group_name].append(d)
            # add file is in the root group
            elif '/' not in group_name:
                if '' not in struct:
                    struct[''] = []
                struct[''].append(d)

        print(struct.keys())
        print(len(struct))

        with open(cache_path, 'w') as f:
            json.dump(struct, f, indent=4)

    return hf, struct


def load_h5_file(hf, path):
    if path.endswith('.jpg') or path.endswith('.png') or path.endswith('.gif'):
        rtn = Image.open(io.BytesIO(np.array(hf[path])))
    elif path.endswith('.json'):
        rtn = json.loads(np.array(hf[path]).tobytes().decode('utf-8'))
    elif path.endswith('.txt'):
        rtn = np.array(hf[path]).tobytes().decode('utf-8')
    elif path.endswith('.csv'):
        rtn = np.array(hf[path]).tobytes().decode('utf-8')
    elif path.endswith('.mp4') or path.endswith('.avi') or path.endswith('.mov'):
        rtn = np.array(hf[path])
    else:
        raise ValueError('Unknown file type: {}'.format(path))
    return rtn

if __name__ == "__main__":
    hf = h5py.File('./UAV/UAV_CSV1.h5', 'r')
    file = load_h5_file(hf, './all_rgb/P067S08G11B10H20UC072072LC022021A135R0_09112106.avi')
    # file = load_h5_file(hf, '/all_rgb/P118S00G10B00H00UC051000LC021000A073R0_10031527.avi')
    print(str(file)[:10])
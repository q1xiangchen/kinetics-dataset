import os
import h5py
from hdf5_pack import add_to_hdf5
from load_h5 import get_h5_struct

root_dl="k700-2020"
root_dl_targz="k700-2020_targz"

if __name__ == "__main__":
    curr_dl=os.path.join(root_dl_targz, "test")
    curr_extract=os.path.join(root_dl, "test")

    os.makedirs(curr_extract, exist_ok=True)
    tar_list = sorted(os.listdir(curr_dl))
    tar_list_len = len(tar_list)

    # create hdf5 file
    save_path = "k700_2020_test.h5"
    # get_h5_struct(save_path, update=True)

    # divide the process into few parts
    parts = 25
    temp_len = tar_list_len // parts

    for i in range(parts):
        # extract tar files
        start = i * temp_len
        end = (i + 1) * temp_len
        if i == parts - 1:
            end = tar_list_len
        for j in range(start, end):
            tar_file = os.path.join(curr_dl, tar_list[j])
            if not tar_file.endswith(".tar.gz"):
                continue
            print(f"Extracting {tar_file} to {curr_extract} ...")
            os.system(f"tar -xzf {tar_file} -C {curr_extract}")

        # add to hdf5
        extracted_list = sorted(os.listdir(curr_extract))
        update_groups = []
        for folder in extracted_list:
            folder_path = os.path.join(curr_extract, folder)
            if os.path.isdir(folder_path):
                update_groups.append(folder)

        print(f"update_groups: {update_groups}")

        if not os.path.exists(save_path):
            hf = h5py.File(save_path, 'w')
        else:
            hf = h5py.File(save_path, 'a')


        add_to_hdf5(hf, curr_extract, update_groups)

        if i % 10 == 0 or i == parts - 1:
            get_h5_struct(save_path, update=True)

        hf.close()

        # remove extracted files
        os.system(f"rm -rf {curr_extract}/*")


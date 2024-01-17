import json
import os

cur_dir = os.getcwd()
company_file_path = os.path.join(cur_dir, "data/mud_lead.json")
sam_file_path = os.path.join(cur_dir, "data/sam.json")
deed_file_path = os.path.join(cur_dir, "data/deeds.json")


def load_data_from_json(file_path):
    with open(file_path, 'r') as file:
        data = json.load(file)
    return data


# company_data = load_data_from_json(company_file_path)
# sam_data = load_data_from_json(sam_file_path)
# deed_data = load_data_from_json(deed_file_path)


import json
import os

records = []
file_list = [f'nhtsa_data/nhtsa_{year}.json' for year in range(1993, 2026)] + ['nhtsa_data/nhtsa_unknown.json']

for file_path in file_list:
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                for record in data:
                    is_frontal = False
                    test_title = record.get('test_title', '').upper()
                    test_type = record.get('test_type', '').upper()
                    test_config = record.get('test_config', '').upper()
                    crash_angle_deg = record.get('crash_angle_deg')

                    if 'FRONTAL' in test_title:
                        is_frontal = True
                    elif 'FMVSS 208' in test_type:
                        is_frontal = True
                    elif test_config == 'VEHICLE INTO BARRIER' and (crash_angle_deg is None or crash_angle_deg == 0.0):
                        is_frontal = True

                    if is_frontal:
                        records.append({
                            'test_no': record.get('test_no'),
                            'test_title': record.get('test_title'),
                            'make': record.get('make'),
                            'model': record.get('model'),
                            'model_year': record.get('model_year')
                        })
        except json.JSONDecodeError:
            print(f"Error decoding JSON from {file_path}", file=os.sys.stderr)
        except Exception as e:
            print(f"An error occurred while processing {file_path}: {e}", file=os.sys.stderr)

print(json.dumps(records, ensure_ascii=False, indent=4))

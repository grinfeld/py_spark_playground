import argparse
import yaml

def deep_merge(dict1, dict2):
    for key, value in dict2.items():
        if key in dict1 and isinstance(dict1[key], dict) and isinstance(value, dict):
            dict1[key] = deep_merge(dict1[key], value)
        elif key in dict1 and isinstance(dict1[key], list) and isinstance(value, list):
            # Example: Extend the list in dict1 with elements from value
            dict1[key].extend(value)
            # Or, replace the list entirely:
            # dict1[key] = value
        else:
            dict1[key] = value
    return dict1

def dump_yaml(filepath, data):
    with open(filepath, 'w') as file:
        yaml.dump(data, file, default_flow_style=False, sort_keys=False)

def open_file_for_read(path: str):
    with open(path, 'r') as file:
        original_yaml = yaml.safe_load(file)
        return original_yaml
def generate_new_values(values: str, original_yaml):
    override_data = yaml.safe_load(values)

    merged = deep_merge(original_yaml, override_data)
    if "service" in merged and "ports" in merged["service"]:
        merged["service"]["port"] = None
    return merged

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("--release-name", "-r", help="Release Name", required=True)
        parser.add_argument("--values", "-v", help="Value to override in values yaml", required=True)
        parser.add_argument("--path", "-p", help="Path To values.yaml for specific release", required=False)
        args = parser.parse_args()

        path = f"{args.release_name}/values.yaml" if args.path is None else args.path
        original_yml = open_file_for_read(path)
        new_yml = generate_new_values(args.values, original_yml)
        if original_yml == new_yml:
            dump_yaml(path, new_yml)
            print("true")
        else:
            print("false")
    except Exception as e:
        print(str(e))
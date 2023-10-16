import os

for file in os.listdir("output/missing_buildings/"):
    old_path = os.path.join("output/missing_buildings/", file)
    new_path = os.path.join(f"output/missing_buildings_merged/", os.path.basename(old_path).split("-")[0], os.path.basename(old_path))
    os.makedirs(os.path.dirname(new_path), exist_ok=True)
    os.rename(old_path, new_path)
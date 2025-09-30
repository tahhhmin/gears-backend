import sys
from netCDF4 import Dataset

def explore(path):
    print(f"📂 Opening {path}")
    ds = Dataset(path, "r")

    def walk(group, indent=0):
        space = "  " * indent
        print(f"{space}📁 Group: {group.path}")
        if group.dimensions:
            print(f"{space}  🔹 Dimensions:")
            for dname, dim in group.dimensions.items():
                print(f"{space}    - {dname}: size={len(dim)}")
        if group.variables:
            print(f"{space}  🔹 Variables:")
            for vname, var in group.variables.items():
                print(f"{space}    - {vname}: shape={var.shape}, dtype={var.dtype}")
        # recurse into subgroups
        for sub in group.groups.values():
            walk(sub, indent+1)

    walk(ds)
    ds.close()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python nc_explorer.py <file.nc>")
        sys.exit(1)
    explore(sys.argv[1])

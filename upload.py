from datasets import load_dataset
import glob

ds = load_dataset("json", data_files=glob.glob("./output/*.jsonl"))

ds = ds.rename_column("html", "text")

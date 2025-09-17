import os
import shutil
import sys

def copy_with_rename(src_dir, dest_dir, skip_errors=True):
    """
    Copy files from src_dir to dest_dir.
    If a file has the same name as its parent folder, rename it.
    If skip_errors=True, log and continue on errors instead of crashing.
    """

    for root, _, files in os.walk(src_dir):
        for file in files:
            src_file = os.path.join(root, file)

            parent_folder = os.path.basename(root)
            filename, ext = os.path.splitext(file)

            # Relative path to preserve structure
            rel_path = os.path.relpath(root, src_dir)
            target_folder = os.path.join(dest_dir, rel_path)
            os.makedirs(target_folder, exist_ok=True)

            # Default destination
            dest_file = os.path.join(target_folder, file)

            # Rename if filename == parent folder
            if filename == parent_folder:
                dest_file = os.path.join(target_folder, f"{filename}_file{ext}")

            try:
                shutil.copy2(src_file, dest_file)
                print(f"Copied: {src_file} -> {dest_file}")
            except Exception as e:
                msg = f"Error copying {src_file} -> {dest_file}: {e}"
                if skip_errors:
                    print(f"⚠️  {msg} (skipped)")
                    continue
                else:
                    raise

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python copy_with_rename.py <source_folder> <destination_folder> [--no-skip]")
        sys.exit(1)

    src = sys.argv[1]
    dest = sys.argv[2]
    skip = True if len(sys.argv) < 4 or sys.argv[3] != "--no-skip" else False

    copy_with_rename(src, dest, skip_errors=skip)
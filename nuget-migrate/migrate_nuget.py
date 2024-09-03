import argparse
import base64
import os
import logging
import requests
import subprocess

logging.basicConfig(level=logging.INFO)
def get_packages_list(pat:str, url:str):

    headers = {
        'Accept': 'application/json',
        'Authorization': 'Basic '+pat
    }
    package_list = []

    try:
        response = requests.get(url=url, headers=headers)
        if response.status_code == 200:
            packages = response.json()["value"]
            for package in packages:
                package_name = package["name"]
                package_name_norm = package["normalizedName"]
                versions = [version["normalizedVersion"] for version in package["versions"]]
                package_list.append({"name": package_name, "name_norm": package_name_norm, "versions": versions})
        else:
            response.raise_for_status()
    except requests.exceptions.RequestException as e:
        logging.error(f"{str(e)}")
    return package_list


def download_package(pat:str, download_url:str,  output_dir:str, package_name:str, package_norm:str, version:str):
    headers = {
        'Authorization': 'Basic '+pat
    }
    url = f"{download_url}/{package_norm}/{version}/{package_norm}.{version}.nupkg"
    package_path = os.path.join(output_dir, f"/{package_name}/{version}")
    if not os.path.exists(package_path):
        os.makedirs(package_path)
    logging.info(f"downloading: {package_norm}.{version}")
    r = requests.get(url=url, headers=headers)
    with open(f"{output_dir}/{package_norm}.{version}.nupkg", "wb") as f:
        f.write(r.content)

    logging.info(f"Finished downloading: {package_norm}.{version}")

def parse_args():
    parser = argparse.ArgumentParser(description="Migrate NuGet packages from Azure DevOps to GitHub")
    parser._action_groups.pop()
    parser.add_argument_group("Required arguments")
    parser.add_argument("--pat", help="Personal Access Token for Azure DevOps", dest="ado_pat", required=True)
    parser.add_argument("--package-list-url", help="ADO package list url", dest="ado_package_list_url", required=True)
    parser.add_argument("--package-download-url", help="ADO package download", dest="ado_package_down_url", required=True)
    parser.add_argument("--gh-pat", help="GH pat", dest="gh_pat", required=True)
    parser.add_argument("--gh-packages-url", help="Github packages url", dest="gh_package-url", required=True)
    parser.add_argument("--output-dir", help="Output directory for NuGet packages", dest="output_dir", default="./nuget_packages")
    return parser.parse_args()

def main():
    args = parse_args()
    personal_access_token = args.ado_pat
    authorization = str(base64.b64encode(bytes(':'+personal_access_token, 'ascii')), 'ascii')
    packages_list = get_packages_list(authorization, args.ado_package_list_url)
    
    output_dir = args.output_dir
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for package in packages_list:
        for version in package["versions"]:
            download_package(authorization, output_dir, package["name"], package["name_norm"], version)

    subprocess.run(["/usr/local/share/dotnet/dotnet",
                    "nuget",
                    "push",
                    "--skip-duplicate",
                    "--timeout", "18000",
                    "--api-key", args.gh_pat,
                    "--source", args.gh_package_url,
                    f"{output_dir}/*.nupkg"
                    ])


if __name__ == "__main__":
    main()

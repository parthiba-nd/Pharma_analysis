from google.cloud import storage

st = storage.Client("neodocs-8d6cd")
bucket = st.bucket("neodocs-8d6cd-utils")

# List all JSON files
json_files = [blob.name for blob in bucket.list_blobs() if blob.name.endswith(".json")]

print(json_files)


#['org_access_codes/ajanta-hb.json', 
# 'org_access_codes/akumentis-hb.json', 
# 'org_access_codes/alembic-uacr.json', 
# 'org_access_codes/benitowa-uacr.json', 
# 'org_access_codes/bi-pharma.json', 
# 'org_access_codes/bluecross-uacr.json',
#  'org_access_codes/cachet-iup-hb.json', 
# 'org_access_codes/coronaremedies-solis-hb.json',
#  'org_access_codes/emcure-gennova.json', 
# 'org_access_codes/emcure-hb.json', 
# 'org_access_codes/fourrts-uti.json', 
# 'org_access_codes/indchemie-hb.json', 
# 'org_access_codes/intas-psa.json', 
# 'org_access_codes/ipca-uacr.json', 
# 'org_access_codes/jb.json', 
# 'org_access_codes/linadapa-uacr.json', 
# 'org_access_codes/lupin-hb-dr-details.json', 
# 'org_access_codes/lupin-hb.json', 
# 'org_access_codes/mankind-hb.json', 
# 'org_access_codes/mankind-zesteva-hb.json', 
# 'org_access_codes/microlabs-ferisome-hb.json', 
# 'org_access_codes/mmc.json', 
# 'org_access_codes/nutricharge-hb.json', 
# 'org_access_codes/reach52-hb.json', 
# 'org_access_codes/sunpharma.json', 
# 'org_access_codes/systopic-hb-pilot.json',
#  'org_access_codes/usv.json', 
# 'org_access_codes/vitamystic-hb.json']

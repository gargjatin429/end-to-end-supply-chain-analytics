import yaml

with open("docker-compose.yml", "r") as f:
    config = yaml.safe_load(f)

# Add the dataset volume mount to airflow-common
volumes = config['x-airflow-common']['volumes']
if '- ./DataCoSupplyChainDataset.csv:/opt/airflow/DataCoSupplyChainDataset.csv' not in volumes:
    volumes.append('./DataCoSupplyChainDataset.csv:/opt/airflow/DataCoSupplyChainDataset.csv')
    volumes.append('./scripts:/opt/airflow/scripts')

config['x-airflow-common']['volumes'] = volumes

with open("docker-compose.yml", "w") as f:
    yaml.dump(config, f, sort_keys=False)

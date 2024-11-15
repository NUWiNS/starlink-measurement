tech_order = ['NO SERVICE', 'LTE', 'LTE-A', '5G-low', '5G-mid', '5G-mmWave (28GHz)', '5G-mmWave (39GHz)']
tech_config_map = {
    'NO SERVICE': {
        'color': '#808080',  # Grey
        'label': 'NO SERVICE'
    },
    'LTE': {
        'color': '#326f21',  # Green
        'label': 'LTE'
    },
    'LTE-A': {
        'color': '#86c84d',  # Light green
        'label': 'LTE-A'
    }, 
    '5G-low': {
        'color': '#ffd700',  # Yellow
        'label': '5G-low'
    },
    '5G-mid': {
        'color': '#ff9900',  # Amber
        'label': '5G-mid'
    },
    '5G-mmWave (28GHz)': {
        'color': '#ff4500',  # Orange
        'label': '5G-mmWave (28GHz)'
    },
    '5G-mmWave (39GHz)': {
        'color': '#ba281c',  # Red
        'label': '5G-mmWave (39GHz)'
    }
}
colors = [tech_config_map[tech]['color'] for tech in tech_order]
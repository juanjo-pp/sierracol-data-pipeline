


schemas_end_points = {

    'ventas': {
        'url': 'https://api.eia.gov/v2/petroleum/pri/spt/data/',
        'params': {
            'frequency': 'daily',
            'length': '5000',
            'sort[0][column]': 'period',
            'sort[0][direction]': 'asc',
            'data[0]': 'value'
        }
    },

    'prices_sales_volumes_stocks': {
        'url': 'https://api.eia.gov/v2/petroleum/sum/mkt/data/',
        'params': {
            'frequency': 'monthly',
            'length': '5000',
            'sort[0][column]': 'period',
            'sort[0][direction]': 'asc',
            'data[0]': 'value'
        }
    },

    'crude_oil_production': {
        'url': 'https://api.eia.gov/v2/petroleum/crd/crpdn/data/',
        'params': {
            'frequency': 'monthly',
            'length': '5000',
            'sort[0][column]': 'period',
            'sort[0][direction]': 'asc',
            'data[0]': 'value'
        }
    }

}
class Config:
    DEBUG = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_DATABASE_URI = 'mssql+pymssql://sa:gladis0083@192.168.0.97/salesorder'

    SQLALCHEMY_BINDS = {
        'exact': 'mssql+pymssql://sa:gladis0083@192.168.0.98/120',
        'salesorder': 'mssql+pymssql://sa:gladis0083@192.168.0.97/salesorder',
        'BI_DM': 'mssql+pymssql://sa:gladis0083@192.168.0.83/BI_DM',
        'sfeed': 'mssql+pymssql://sa:gladis0083@192.168.0.80/sfeed',

    }

class ExactConfig:

    db = {
        'host': '192.168.0.98', 'port': '1433',
        'user': 'sa', 'password':'gladis0083',
        'database': '110',
        #'charset':'utf8'
    }
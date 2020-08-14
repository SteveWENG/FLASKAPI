from flask_restful import Resource
from .parsers import parser

class TODO(Resource):
    def get(self):
        req = parser.parse_args()
        id = req['id']
        return {'id': id}

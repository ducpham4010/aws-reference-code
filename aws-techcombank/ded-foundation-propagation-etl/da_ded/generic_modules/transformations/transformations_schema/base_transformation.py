from marshmallow import Schema, fields


class Transformation(Schema):
    name = fields.String(required=True)
    comment = fields.String()
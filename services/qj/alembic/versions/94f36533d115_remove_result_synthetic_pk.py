"""Remove result synthetic pk

Revision ID: 94f36533d115
Revises: e6e2a6bf2a39
Create Date: 2022-08-16 09:58:18.309009

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '94f36533d115'
down_revision = 'e6e2a6bf2a39'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('result', 'id')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('result', sa.Column('id', sa.INTEGER(), autoincrement=True, nullable=False))
    # ### end Alembic commands ###

from conexao import *
from tqdm import tqdm

from modulos.tools import *
from modulos.patrimonio import base

# CONTAS = plano_contas()
SUBUNIDADES = subunidades()
_, INSMF = fornecedores()

from modulos.patrimonio import cadastro
from modulos.patrimonio import movimentacoes
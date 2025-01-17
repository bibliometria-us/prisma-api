ALTER TABLE prisma.i_investigador ADD idCentroCenso varchar(5) DEFAULT NULL NULL;
ALTER TABLE prisma.i_investigador CHANGE idCentroCenso idCentroCenso varchar(5) DEFAULT NULL NULL AFTER idCentro;
CREATE INDEX i_investigador_idCentroCenso_IDX USING BTREE ON prisma.i_investigador (idCentroCenso);
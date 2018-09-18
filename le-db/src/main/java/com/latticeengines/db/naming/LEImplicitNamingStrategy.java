package com.latticeengines.db.naming;

import java.util.List;

import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.naming.ImplicitForeignKeyNameSource;
import org.hibernate.boot.model.naming.ImplicitNamingStrategyJpaCompliantImpl;


public class LEImplicitNamingStrategy extends ImplicitNamingStrategyJpaCompliantImpl {

    /**
     *
     */
    private static final long serialVersionUID = 4391821679953137277L;
    public static final LEImplicitNamingStrategy INSTANCE = new LEImplicitNamingStrategy();

    @SuppressWarnings("unused")
    @Override
    public Identifier determineForeignKeyName(ImplicitForeignKeyNameSource source) {
        Identifier userProvidedIdentifier = source.getUserProvidedIdentifier();
        Identifier tableName = source.getTableName();
        Identifier referencedTableName = source.getReferencedTableName();
        List<Identifier> columnNames = source.getColumnNames();
        List<Identifier> referencedColumnNames = source.getReferencedColumnNames();
        StringBuilder sb = new StringBuilder("FK_");
        sb.append(tableName.getText().replace("_", ""));

        final Identifier[] columnNamesArray, referencedColumnNamesArray;
        if (columnNames == null || columnNames.isEmpty()) {
            columnNamesArray = new Identifier[0];
        } else {
            columnNamesArray = columnNames.toArray(new Identifier[columnNames.size()]);
        }
        if (referencedColumnNames == null || referencedColumnNames.isEmpty()) {
            referencedColumnNamesArray = new Identifier[0];
        } else {
            referencedColumnNamesArray = referencedColumnNames.toArray(new Identifier[referencedColumnNames.size()]);
        }

        if (columnNames != null && columnNames.size() != 0) {
            sb.append("_");
        }
        for (Identifier columnName : columnNamesArray) {
            sb.append(columnName.getText().replace("_", ""));
        }

        sb.append("_").append(referencedTableName.getText().replace("_", ""));
        // if (referencedColumnNames != null && referencedColumnNames.size() !=
        // 0) {
        // sb.append("_");
        // }
        // for (Identifier columnName : referencedColumnNamesArray) {
        // sb.append(columnName.getText().replace("_", ""));
        // }
        String result = sb.toString();
        return userProvidedIdentifier != null ? userProvidedIdentifier
                : toIdentifier(result.substring(0, Math.min(result.length(), 64)), source.getBuildingContext());
    }
}

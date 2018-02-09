package com.latticeengines.db.naming;

import java.util.List;


import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.naming.ImplicitForeignKeyNameSource;
import org.hibernate.boot.model.naming.ImplicitNamingStrategyJpaCompliantImpl;


public class LEImplicitNamingStrategy extends ImplicitNamingStrategyJpaCompliantImpl {

    /**
     *
     */
    private static Integer num = 0;
    private static final long serialVersionUID = 4391821679953137277L;
    public static final LEImplicitNamingStrategy INSTANCE = new LEImplicitNamingStrategy();

    @Override
    public Identifier determineForeignKeyName(ImplicitForeignKeyNameSource source) {
        Identifier userProvidedIdentifier = source.getUserProvidedIdentifier();
        Identifier referencedTableName = source.getReferencedTableName();
        List<Identifier> columnNames = source.getColumnNames();
        List<Identifier> referencedColumnNames = source.getReferencedColumnNames();

        StringBuilder sb = new StringBuilder();

        sb.append(referencedTableName.getText().replace("_", ""));
        if (columnNames != null && columnNames.size() != 0) {
            sb.append("_");
        }
        for (Identifier columnName : columnNames) {
            sb.append(columnName.getText().replace("_", ""));
        }

        for (Identifier columnName : referencedColumnNames) {
            sb.append(columnName.getText().replace("_", ""));
        }
        if (referencedColumnNames != null && referencedColumnNames.size() != 0) {
            sb.append("_");
        }
        String result = sb.toString();
        return userProvidedIdentifier != null ? userProvidedIdentifier
                : toIdentifier(result.substring(0, Math.min(result.length(), 28)) + (num++) % 100,
                        source.getBuildingContext());
    }
}

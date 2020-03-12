/* jshint indent: 2 */

module.exports = function(sequelize, DataTypes) {
  return sequelize.define('METADATA_ATTRIBUTE', {
    PID: {
      type: DataTypes.BIGINT,
      allowNull: false,
      primaryKey: true,
      autoIncrement: true
    },
    ENUM_VALUES: {
      type: DataTypes.STRING(2048),
      allowNull: true
    },
    DISPLAY_NAME: {
      type: DataTypes.STRING(255),
      allowNull: false
    },
    LENGTH: {
      type: DataTypes.INTEGER(11),
      allowNull: true
    },
    LOGICAL_DATA_TYPE: {
      type: DataTypes.STRING(255),
      allowNull: true
    },
    NAME: {
      type: DataTypes.STRING(255),
      allowNull: false
    },
    NULLABLE: {
      type: DataTypes.INTEGER(1),
      allowNull: false
    },
    DATA_TYPE: {
      type: DataTypes.STRING(255),
      allowNull: false
    },
    PRECISION: {
      type: DataTypes.INTEGER(11),
      allowNull: true
    },
    PROPERTIES: {
      type: "LONGBLOB",
      allowNull: false
    },
    SCALE: {
      type: DataTypes.INTEGER(11),
      allowNull: true
    },
    SECONDARY_DISPLAY_NAME: {
      type: DataTypes.STRING(1000),
      allowNull: true
    },
    SOURCE_LOGICAL_DATA_TYPE: {
      type: DataTypes.STRING(255),
      allowNull: true
    },
    TENANT_ID: {
      type: DataTypes.BIGINT,
      allowNull: false
    },
    VALIDATORS: {
      type: "LONGBLOB",
      allowNull: true
    },
    FK_TABLE_ID: {
      type: DataTypes.BIGINT,
      allowNull: false,
      references: {
        model: 'METADATA_TABLE',
        key: 'PID'
      }
    },
    GROUPS: {
      type: DataTypes.STRING(1000),
      allowNull: true
    }
  }, {
    tableName: 'METADATA_ATTRIBUTE',
    timestamps: false,
    logging: false
  });
};

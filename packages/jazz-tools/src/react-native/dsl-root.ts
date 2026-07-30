// React Native entrypoint for the schema DSL.
//
// The default `.` export pulls in the Node runtime, which React Native cannot load, so a
// bundler resolving `jazz-tools` under the `react-native` condition lands here instead and
// gets the schema-definition surface on its own.
import { col } from "../dsl.js";
import { defineMigration, renameTableFrom } from "../migrations.js";
import { definePermissions } from "../permissions/index.js";
import {
  defineApp,
  defineSchema,
  defineSliceableApp,
  defineTable,
  permissionIntrospectionColumns,
  TypedTableQueryBuilder,
} from "../typed-app.js";
import { generateAuthSecret } from "../runtime/auth-secret-store.js";

export { col, defineApp, defineSchema, defineSliceableApp, TypedTableQueryBuilder, permissionIntrospectionColumns };
export { defineMigration, renameTableFrom } from "../migrations.js";
export { table, migrate, getCollectedSchema, getCollectedMigration, resetCollectedState } from "../dsl.js";
export * from "../permissions/index.js";
export { generateAuthSecret };

// Annotated with a locally-nameable type, exactly as the root index does. Without it
// declaration emit fails with TS4023: the inferred type reaches a `$JSONSchema` from
// json-schema-to-ts that cannot be named in the output. The original pnpm patch never hit
// this because it edited the built dist and shipped a hand-written .d.ts.
type RuntimeSchemaNamespace = typeof col & {
  table: typeof defineTable;
  defineSchema: typeof defineSchema;
  defineApp: typeof defineApp;
  defineSliceableApp: typeof defineSliceableApp;
  defineMigration: typeof defineMigration;
  renameTableFrom: typeof renameTableFrom;
  definePermissions: typeof definePermissions;
  permissionIntrospectionColumns: typeof permissionIntrospectionColumns;
};

export const schema: RuntimeSchemaNamespace = Object.assign({}, col, {
  table: defineTable,
  defineSchema,
  defineApp,
  defineSliceableApp,
  defineMigration,
  renameTableFrom,
  definePermissions,
  permissionIntrospectionColumns,
});

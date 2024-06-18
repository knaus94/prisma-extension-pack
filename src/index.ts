import { Prisma as PrismaExtension } from "@prisma/client/extension";

type Pagination = {
  take?: number;
  skip?: number;
};

export default () => {
  return PrismaExtension.defineExtension({
    name: "pack",
    model: {
      $allModels: {
        async findRandom<T, A>(
          this: T,
          args?: PrismaExtension.Exact<
            A,
            PrismaExtension.Args<T, "findFirst">
          > &
            object
        ) {
          const context = PrismaExtension.getExtensionContext(this);

          const numRows = (await (context as any).count({
            where: (args as { where?: object } | undefined)?.where,
          })) as number;
          return (await (context as any).findFirst({
            ...args,
            skip: Math.max(0, Math.floor(Math.random() * numRows)),
          })) as PrismaExtension.Result<T, A, "findFirst">;
        },

        async findManyRandom<T, TWhere, TSelect>(
          this: T,
          num: number,
          args?: {
            where?: PrismaExtension.Exact<
              TWhere,
              PrismaExtension.Args<T, "findFirst">["where"]
            >;
            select?: PrismaExtension.Exact<
              TSelect,
              PrismaExtension.Args<T, "findFirst">["select"] & { id: true }
            >;
          }
        ) {
          const context = PrismaExtension.getExtensionContext(this);
          type FindFirstResult = PrismaExtension.Result<
            T,
            { where: TWhere; select: TSelect },
            "findFirst"
          >;

          const select = args?.select ?? { id: true as const };
          let where = args?.where ?? {};

          let numRows = (await (context as any).count({ where })) as number;

          const rows: Array<NonNullable<FindFirstResult>> = [];
          const rowIds: string[] = [];

          where = {
            ...where,
            id: { notIn: rowIds },
          };

          for (let i = 0; i < num && numRows > 0; ++i) {
            const row = (await (context as any).findFirst({
              select,
              where,
              skip: Math.max(0, Math.floor(Math.random() * numRows)),
            })) as FindFirstResult;

            if (!row) {
              console.error(
                `get random row failed. Where clause: ${JSON.stringify(where)}`
              );
              break;
            }
            rows.push(row);
            rowIds.push((row as unknown as { id: string }).id);
            numRows--;
          }

          return rows;
        },

        async exists<T>(
          this: T,
          where: PrismaExtension.Args<T, "findFirst">["where"]
        ): Promise<boolean> {
          const context = PrismaExtension.getExtensionContext(this);

          const result = await (context as any).findFirst({ where });
          return result !== null;
        },

        async paginate<T, A>(
          this: T,
          args?: PrismaExtension.Exact<
            A,
            PrismaExtension.Args<T, "findMany">
          > & {
            pagination?: Pagination;
          }
        ) {
          const context = PrismaExtension.getExtensionContext(this);
          const { pagination, ...operationArgs } = (args ?? {}) as any;
          const take = args?.pagination?.take ?? operationArgs?.take ?? 10;
          const skip = args?.pagination?.skip ?? operationArgs?.skip ?? 0;

          const [data, total]: [
            PrismaExtension.Result<T, A, "findMany">,
            number
          ] = await Promise.all([
            (context as any).findMany({
              ...operationArgs,
              skip,
              take,
            }),
            (context as any).count({ where: operationArgs?.where }),
          ]);

          return {
            data,
            total,
          };
        },
        async updateIgnoreOnNotFound<T, A>(
          this: T,
          args: PrismaExtension.Exact<A, PrismaExtension.Args<T, "update">>
        ): Promise<PrismaExtension.Result<T, A, "update"> | null> {
          try {
            const context = PrismaExtension.getExtensionContext(this) as any;
            return await context.update(args);
          } catch (err) {
            if (err?.code === "P2025") {
              return null;
            }
            throw err;
          }
        },
        async deleteIgnoreOnNotFound<T, A>(
          this: T,
          args: PrismaExtension.Exact<A, PrismaExtension.Args<T, "delete">>
        ): Promise<PrismaExtension.Result<T, A, "delete"> | null> {
          try {
            const context = PrismaExtension.getExtensionContext(this) as any;
            return await context.delete(args);
          } catch (err) {
            if (err?.code === "P2025") {
              return null;
            }
            throw err;
          }
        },
      },
    },
  });
};

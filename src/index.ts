import { Prisma } from "@prisma/client/extension";

type Pagination = {
  take?: number;
  skip?: number;
};

export default () => {
  return Prisma.defineExtension({
    name: "pack",
    model: {
      $allModels: {
        async findRandom<T, A>(
          this: T,
          args?: Prisma.Exact<A, Prisma.Args<T, "findFirst">> & object
        ) {
          const context = Prisma.getExtensionContext(this);

          const numRows = (await (context as any).count({
            where: (args as { where?: object } | undefined)?.where,
          })) as number;
          return (await (context as any).findFirst({
            ...args,
            skip: Math.max(0, Math.floor(Math.random() * numRows)),
          })) as Prisma.Result<T, A, "findFirst">;
        },

        async findManyRandom<T, TWhere, TSelect>(
          this: T,
          num: number,
          args?: {
            where?: Prisma.Exact<TWhere, Prisma.Args<T, "findFirst">["where"]>;
            select?: Prisma.Exact<
              TSelect,
              Prisma.Args<T, "findFirst">["select"] & { id: true }
            >;
          }
        ) {
          const context = Prisma.getExtensionContext(this);
          type FindFirstResult = Prisma.Result<
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
          where: Prisma.Args<T, "findFirst">["where"]
        ): Promise<boolean> {
          const context = Prisma.getExtensionContext(this);

          const result = await (context as any).findFirst({ where });
          return result !== null;
        },

        async paginate<T, A>(
          this: T,
          args?: Prisma.Exact<A, Prisma.Args<T, "findMany">> & {
            pagination?: Pagination;
          }
        ) {
          const context = Prisma.getExtensionContext(this);
          const { pagination, ...operationArgs } = (args ?? {}) as any;
          const take = args?.pagination?.take ?? 10;
          const skip = args?.pagination?.skip ?? 0;

          const [data, total]: [Prisma.Result<T, A, "findMany">, number] =
            await Promise.all([
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
      },
    },
  });
};

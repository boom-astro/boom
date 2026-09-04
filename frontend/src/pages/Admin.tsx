// A page for administrative work like managing users, ingesting new catalogs,
// kicking off reprocessing, etc.


function CatalogsTable() {
    return (
        <div>
            <h2>Catalogs</h2>
        </div>
    )
}

export default function Admin() {
    return (
        <div className="px-4 lg:px-6">
            <div className="max-w-3xl mx-auto">
                <h1 className="text-2xl font-bold mb-4">Admin</h1>
                <p className="text-sm text-muted-foreground mb-4">
                    This page is for administrative tasks like managing users, ingesting new catalogs, and kicking off reprocessing.
                </p>
            </div>
            <CatalogsTable />
        </div>
    )
}

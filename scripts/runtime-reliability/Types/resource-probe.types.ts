/** Numeric resource sampling reads metadata only; descriptor targets are never inspected. */
export type ResourceProcFilesystem = {
    readdir(path: string): Promise<string[]>
    readFile(path: string, encoding: 'utf8'): Promise<string>
}

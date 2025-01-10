
//Estado General

interface StatusBreakdownProps {
  data: {
    label: string;
    count: number;
    color: string;
  }[];
}

export function StatusBreakdown({ data }: StatusBreakdownProps) {
  const total = data.reduce((acc, item) => acc + item.count, 0);

  return (
    <div className="bg-white rounded-lg shadow p-6">
      <h3 className="text-lg font-medium text-gray-900 mb-4">Estado General</h3>
      <div className="space-y-4">
        {data.map((item, index) => (
          <div key={index}>
            <div className="flex justify-between mb-1">
              <span className="text-sm font-medium text-gray-700">{item.label}</span>
              <span className="text-sm font-medium text-gray-900">
                {item.count} ({Math.round((item.count / total) * 100)}%)
              </span>
            </div>
            <div className="w-full bg-gray-200 rounded-full h-2.5">
              <div
                className={`${item.color} h-2.5 rounded-full`}
                style={{ width: `${(item.count / total) * 100}%` }}
              ></div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
import { PropsWithChildren } from "react";


export default function ErrorMesage({children}: PropsWithChildren) {
  return (
    <div className="text-center my-4 bg-red-600 text-white p-3 font-bold uppercase">
        {children}
    </div>
  )
}
